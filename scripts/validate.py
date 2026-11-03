#!/usr/bin/env python3
"""Offline Kustomize/ownership/routing checks. --ready also rejects placeholder deployment inputs."""
import argparse
from pathlib import Path
import subprocess
import sys
import yaml
ROOT = Path(__file__).resolve().parents[1]

def build(path):
    result = subprocess.run(['kubectl','kustomize',str(path)],check=True,text=True,capture_output=True)
    return [d for d in yaml.safe_load_all(result.stdout) if d]

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ready',action='store_true')
    p.add_argument('--argocd-schema',type=Path,help='Optional checksum-verified official Argo CD install manifest')
    args=p.parse_args()
    for path in ROOT.rglob('*.yaml'):
        if '.venv' not in path.parts and '.build' not in path.parts:
            list(yaml.safe_load_all(path.read_text()))
    bootstrap=build(ROOT/'argocd/bootstrap')
    applications=build(ROOT/'argocd/applications')
    if args.argocd_schema:
        import jsonschema
        schemas={}
        for obj in yaml.safe_load_all(args.argocd_schema.read_text()):
            if obj and obj.get('kind')=='CustomResourceDefinition' and obj['spec']['group']=='argoproj.io':
                version=next(v for v in obj['spec']['versions'] if v['storage'])
                schemas[obj['spec']['names']['kind']]=version['schema']['openAPIV3Schema']
        assert {'Application','AppProject'} <= set(schemas), 'Missing official Argo CRD schemas'
        for obj in applications+bootstrap:
            if obj['kind'] in schemas:
                jsonschema.Draft7Validator(schemas[obj['kind']]).validate(obj)
        print('PASS: Applications/AppProject match supplied official Argo CRD schemas')
    names=[a['metadata']['name'] for a in applications]
    assert len(names)==len(set(names)), 'Duplicate Argo Application names'
    assert {'gpu-foundation','gpu-frontend','gpu-backend','gpu-web','gpu-multikueue'} <= set(names)
    seen=set()
    for app in applications:
        name=app['metadata']['name']
        spec=app['spec']
        cluster=spec['destination'].get('server',spec['destination'].get('name'))
        if 'sources' in spec:
            chart,values=spec['sources']
            file=chart['helm']['valueFiles'][0].replace('$values/','')
            assert (ROOT/file).is_file(), file
            assert chart['targetRevision'] not in ['main','latest','HEAD']
            continue
        path=ROOT/spec['source']['path']
        assert (path/'kustomization.yaml').is_file(), path
        resources=build(path)
        for obj in resources:
            key=(cluster,obj['apiVersion'],obj['kind'],obj['metadata'].get('namespace',''),obj['metadata']['name'])
            assert key not in seen, ('Two Applications own the same object',key)
            seen.add(key)
            assert obj['kind'] not in ['Secret','JobSet'], 'Credentials/runtime jobs must not be Git desired state'
        if name in ['gpu-frontend','gpu-backend']:
            deployment=next(d for d in resources if d['kind']=='Deployment')
            service=next(d for d in resources if d['kind']=='Service')
            pod=deployment['spec']['template']
            assert service['spec']['type']=='ClusterIP'
            assert service['spec']['selector']==pod['metadata']['labels']
            assert pod['spec']['nodeSelector']=={'platform-worker':'true'}
            if name=='gpu-backend':
                assert not any(d['kind']=='Ingress' for d in resources)
            if args.ready:
                assert deployment['spec']['replicas']>0, name+' image is not configured'
                assert 'replace-me' not in pod['spec']['containers'][0]['image']
                assert '@sha256:' in pod['spec']['containers'][0]['image']
        if name.startswith('gpu-queues-'):
            cq=next(d for d in resources if d['kind']=='ClusterQueue')
            quota=next(q for q in cq['spec']['resourceGroups'][0]['flavors'][0]['resources'] if q['name']=='nvidia.com/gpu')
            assert quota['nominalQuota']==4
    web=build(ROOT/'platform/web')
    ingress=next(d for d in web if d['kind']=='Ingress')
    assert ingress['spec']['rules'][0]['host']=='devoops.site'
    assert ingress['spec']['rules'][0]['http']['paths'][0]['backend']['service']['name']=='frontend'
    if args.ready:
        for app in applications+[d for d in bootstrap if d['kind']=='Application']:
            for source in app['spec'].get('sources',[app['spec'].get('source',{})]):
                assert 'YOUR_ORG' not in source['repoURL'], 'GitHub URL is not configured'
        issuer=next(d for d in web if d['kind']=='ClusterIssuer')
        assert issuer['spec']['acme']['email']!='replace-me@example.com'
    print('PASS: YAML, Kustomize, unique ownership, internal-only backend, ingress and EKS quotas')
    if not args.ready:
        print('Template validation only; run --ready after configuring actual GitHub/images/email.')

if __name__=='__main__':
    try:
        main()
    except (AssertionError,subprocess.CalledProcessError) as e:
        print('VALIDATION FAILED:',e,file=sys.stderr)
        raise SystemExit(1)
