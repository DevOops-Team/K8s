#!/usr/bin/env python3
"""Update one EKS queue and recompute manager admission totals. No API calls or deployment."""
import argparse
import copy
from pathlib import Path
import re
import yaml
ROOT=Path(__file__).resolve().parents[1]
CLUSTERS=['eks-a','eks-b','eks-c']

def quota(doc):
    cq=next(d for d in doc if d and d.get('kind')=='ClusterQueue')
    return {q['name']:q['nominalQuota'] for q in cq['spec']['resourceGroups'][0]['flavors'][0]['resources']}

def memory_gi(value):
    match=re.fullmatch(r'([1-9][0-9]*)Gi',str(value))
    if not match:raise ValueError('Configuration helper expects whole Gi memory, e.g. 128Gi')
    return int(match.group(1))

def configure(cluster,settings):
    if settings.get('cluster_alias')!=cluster:raise ValueError('Settings alias does not match selected EKS')
    labels=settings.get('gpu_node_labels')
    if not isinstance(labels,dict) or not labels or not all(isinstance(k,str) and isinstance(v,str) for k,v in labels.items()):
        raise ValueError('gpu_node_labels must be a nonempty string-to-string mapping')
    gpu=settings.get('gpu_quota');cpu=settings.get('cpu_quota');memory=settings.get('memory_quota')
    if type(gpu) is not int or not 1<=gpu<=4:raise ValueError('GPU quota must be 1..4; fixed design retains capacity outside queue quota')
    if type(cpu) is not int or cpu<1:raise ValueError('CPU quota must be positive whole CPUs')
    memory_gi(memory)
    annotations=settings.get('training_service_account_annotations',{})
    if not isinstance(annotations,dict) or not all(isinstance(k,str) and isinstance(v,str) for k,v in annotations.items()):
        raise ValueError('ServiceAccount annotations must contain strings')
    taints=settings.get('gpu_node_taints',[])
    if not isinstance(taints,list):raise ValueError('gpu_node_taints must be a list')
    for taint in taints:
        if not isinstance(taint,dict) or not taint.get('key') or taint.get('effect') not in ['NoSchedule','PreferNoSchedule','NoExecute']:
            raise ValueError('Invalid GPU node taint')
    workers={name:list(yaml.safe_load_all((ROOT/f'eks/{name}/resources.yaml').read_text())) for name in CLUSTERS}
    target=workers[cluster]
    for doc in target:
        if doc['kind']=='ResourceFlavor':
            doc['spec']['nodeLabels']=labels
            if taints:doc['spec']['nodeTaints']=taints
            else:doc['spec'].pop('nodeTaints',None)
        if doc['kind']=='ServiceAccount' and doc['metadata']['name']=='training':doc['metadata']['annotations']=annotations
        if doc['kind']=='ClusterQueue':
            for q in doc['spec']['resourceGroups'][0]['flavors'][0]['resources']:
                q['nominalQuota']={'cpu':cpu,'memory':memory,'nvidia.com/gpu':gpu}[q['name']]
    totals={'cpu':0,'memory':0,'nvidia.com/gpu':0}
    for docs in workers.values():
        current=quota(docs)
        totals['cpu']+=int(current['cpu']);totals['memory']+=memory_gi(current['memory']);totals['nvidia.com/gpu']+=int(current['nvidia.com/gpu'])
    totals['memory']=str(totals['memory'])+'Gi'
    manager_path=ROOT/'onprem/scheduling/resources.yaml'
    manager=list(yaml.safe_load_all(manager_path.read_text()))
    for doc in manager:
        if doc['kind']=='ClusterQueue':
            for q in doc['spec']['resourceGroups'][0]['flavors'][0]['resources']:q['nominalQuota']=totals[q['name']]
    # Validate everything before either file is written.
    output=lambda docs:'---\n'.join(yaml.safe_dump(d,sort_keys=False,width=110) for d in docs if d)
    rendered_target=output(target);rendered_manager=output(manager)
    (ROOT/f'eks/{cluster}/resources.yaml').write_text(rendered_target)
    manager_path.write_text(rendered_manager)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--cluster',choices=CLUSTERS,required=True);p.add_argument('--settings',type=Path,required=True)
    a=p.parse_args()
    try:configure(a.cluster,yaml.safe_load(a.settings.read_text()))
    except (ValueError,KeyError,TypeError) as e:p.error(str(e))
    print('Updated '+a.cluster+' and manager totals locally. Review both diffs; sync them separately.')
if __name__=='__main__':main()
