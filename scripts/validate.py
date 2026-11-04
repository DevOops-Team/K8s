#!/usr/bin/env python3
"""Offline target-specific builds, ownership, destination and quota checks; never calls cluster APIs."""
import argparse
from pathlib import Path
import subprocess
import sys
import yaml
ROOT=Path(__file__).resolve().parents[1]
TARGETS=['onprem','eks-a','eks-b','eks-c']

def build(path):
    p=subprocess.run(['kubectl','kustomize',str(path)],check=True,text=True,capture_output=True)
    return [d for d in yaml.safe_load_all(p.stdout) if d]

def suffix(target):return 'onprem' if target=='onprem' else 'eks/'+target

def applications(target):return build(ROOT/'argocd/applications'/suffix(target))

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target',choices=['all']+TARGETS,default='all');p.add_argument('--ready',action='store_true')
    p.add_argument('--argocd-schema',type=Path)
    args=p.parse_args();targets=TARGETS if args.target=='all' else [args.target]
    bootstrap=[];apps=[]
    for target in targets:
        bs=build(ROOT/'argocd/bootstrap'/suffix(target));aa=applications(target)
        bootstrap+=bs;apps+=aa
        root=next(d for d in bs if d['kind']=='Application')
        assert root['spec']['source']['path']=='argocd/applications/'+suffix(target)
        project=next(d for d in bs if d['kind']=='AppProject')
        if target=='onprem':assert all(d.get('server')=='https://kubernetes.default.svc' for d in project['spec']['destinations'])
        else:assert all(d.get('name')==target for d in project['spec']['destinations'])
        for app in aa:
            if target=='onprem':assert app['spec']['destination'].get('server')=='https://kubernetes.default.svc'
            else:assert app['spec']['destination'].get('name')==target
    assert len({a['metadata']['name'] for a in apps})==len(apps)
    if args.argocd_schema:
        import jsonschema
        schemas={}
        for obj in yaml.safe_load_all(args.argocd_schema.read_text()):
            if obj and obj.get('kind')=='CustomResourceDefinition' and obj['spec']['group']=='argoproj.io':
                v=next(v for v in obj['spec']['versions'] if v['storage']);schemas[obj['spec']['names']['kind']]=v['schema']['openAPIV3Schema']
        assert {'Application','AppProject'}<=set(schemas)
        for obj in apps+bootstrap:
            if obj['kind'] in schemas:jsonschema.Draft7Validator(schemas[obj['kind']]).validate(obj)
    seen=set()
    for app in apps:
        name=app['metadata']['name'];spec=app['spec'];cluster=spec['destination'].get('name',spec['destination'].get('server'))
        sources=spec.get('sources',[spec.get('source',{})])
        if args.ready:
            assert all('YOUR_ORG' not in s['repoURL'] for s in sources),'Unconfigured GitHub URL'
        if 'sources' in spec:
            chart,values=sources
            assert (ROOT/chart['helm']['valueFiles'][0].removeprefix('$values/')).is_file()
            assert chart['targetRevision'] not in ['main','latest','HEAD']
            continue
        resources=build(ROOT/spec['source']['path'])
        for obj in resources:
            key=(cluster,obj['apiVersion'],obj['kind'],obj['metadata'].get('namespace',''),obj['metadata']['name'])
            assert key not in seen,('Duplicate owner',key)
            seen.add(key)
            assert obj['kind'] not in ['Secret','JobSet'],'Credentials and runtime jobs are not Git resources'
        if name in ['gpu-frontend','gpu-backend']:
            deployment=next(d for d in resources if d['kind']=='Deployment');service=next(d for d in resources if d['kind']=='Service')
            pod=deployment['spec']['template']
            assert service['spec']['type']=='ClusterIP';assert service['spec']['selector']==pod['metadata']['labels']
            assert pod['spec']['nodeSelector']=={'platform-worker':'true'}
            if name=='gpu-backend':assert not any(d['kind']=='Ingress' for d in resources)
            if args.ready:
                assert deployment['spec']['replicas']>0,name+' has no actual image configured'
                assert 'replace-me' not in pod['spec']['containers'][0]['image']
                assert '@sha256:' in pod['spec']['containers'][0]['image']
        if name.startswith('gpu-queues-'):
            cq=next(d for d in resources if d['kind']=='ClusterQueue')
            q=next(q for q in cq['spec']['resourceGroups'][0]['flavors'][0]['resources'] if q['name']=='nvidia.com/gpu')
            assert type(q['nominalQuota']) is int and 1<=q['nominalQuota']<=4
            assert any(d['kind']=='ResourceFlavor' and d['spec']['nodeLabels'] for d in resources)
    if 'onprem' in targets:
        web=build(ROOT/'onprem/platform/web');ingress=next(d for d in web if d['kind']=='Ingress')
        assert ingress['spec']['rules'][0]['host']=='devoops.site'
        assert ingress['spec']['rules'][0]['http']['paths'][0]['backend']['service']['name']=='frontend'
        if args.ready:assert next(d for d in web if d['kind']=='ClusterIssuer')['spec']['acme']['email']!='replace-me@example.com'
    if args.target=='all':
        from importlib.util import spec_from_file_location,module_from_spec
        spec=spec_from_file_location('eks_settings',ROOT/'scripts/configure-eks.py');module=module_from_spec(spec);spec.loader.exec_module(module)
        workers=[module.quota(build(ROOT/'eks'/n)) for n in TARGETS[1:]]
        manager=module.quota(build(ROOT/'onprem/scheduling'))
        assert manager['nvidia.com/gpu']==sum(w['nvidia.com/gpu'] for w in workers)
        assert int(manager['cpu'])==sum(int(w['cpu']) for w in workers)
        assert module.memory_gi(manager['memory'])==sum(module.memory_gi(w['memory']) for w in workers)
    print('PASS: '+','.join(targets)+' — roots/destinations/builds/ownership/quotas')
    print('Static validation only; actual AWS/API/GPU/Secret prerequisites require the target EKS preflight.')
if __name__=='__main__':
    try:main()
    except (AssertionError,subprocess.CalledProcessError) as e:
        print('VALIDATION FAILED:',e,file=sys.stderr);raise SystemExit(1)
