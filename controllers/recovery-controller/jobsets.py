"""Shared helper for the execution service: change only ADR-12 recovery fields."""
import copy
import hashlib
import json
import re


def render_recovery(raw, sha256, job_id, generation, recovery_id, gpu_count):
    if hashlib.sha256(raw).hexdigest()!=sha256:
        raise ValueError('submitted jobset.json checksum mismatch')
    obj=json.loads(raw)
    if obj.get('kind')!='JobSet' or not obj.get('apiVersion','').startswith('jobset.x-k8s.io/'):
        raise ValueError('expected stored JobSet submission')
    meta=obj.get('metadata',{})
    if meta.get('labels',{}).get('devoops/job-id')!=job_id or meta.get('namespace')!='training':
        raise ValueError('job identity mismatch')
    if any(k in meta for k in ('uid','resourceVersion','deletionTimestamp')) or 'status' in obj:
        raise ValueError('use original submission, not live cluster object')
    if type(generation) is not int or generation<2 or not recovery_id:
        raise ValueError('new generation/recovery_id required')
    result=copy.deepcopy(obj)
    name='job-'+job_id+'-g'+format(generation,'04d')
    result['metadata']['name']=name
    result['metadata']['labels'].update({'devoops/generation':str(generation),
        'kueue.x-k8s.io/queue-name':'queue-recovery','kueue.x-k8s.io/priority-class':'recovery'})
    result['metadata'].setdefault('annotations',{})['devoops/recovery-id']=recovery_id
    result['spec']['network']['subdomain']=name
    if result['spec'].get('failurePolicy',{}).get('maxRestarts')!=0:
        raise ValueError('JobSet automatic restart must be disabled')
    jobs=result['spec']['replicatedJobs']
    if len(jobs)!=1 or jobs[0].get('replicas')!=1:
        raise ValueError('expected one Indexed Job with one replica')
    jobs[0]['template'].setdefault('metadata',{}).setdefault('labels',{})['devoops/generation']=str(generation)
    job=jobs[0]['template']['spec']
    if type(gpu_count) is not int or not 1<=gpu_count<=4 or job.get('parallelism')!=gpu_count or job.get('completions')!=gpu_count:
        raise ValueError('original Pod count must match registered GPU count')
    if job.get('backoffLimit')!=0 or job.get('completionMode')!='Indexed':
        raise ValueError('expected Indexed Job with backoffLimit=0')
    pod=job['template']['spec']
    if pod.get('restartPolicy')!='Never':
        raise ValueError('trainer restartPolicy must be Never')
    job['template']['metadata']['labels']['devoops/generation']=str(generation)
    containers=pod.get('initContainers',[])+pod.get('containers',[])
    for required in ('ckpt-init','ckpt-sidecar','progress-reporter','trainer'):
        c=next((c for c in containers if c['name']==required),None)
        if c is None: raise ValueError('missing '+required)
        env=next((v for v in c.get('env',[]) if v['name']=='GENERATION'),None)
        if env is None or 'valueFrom' in env: raise ValueError('literal GENERATION required')
        env['value']=str(generation)
        if required in ('ckpt-init','ckpt-sidecar'):
            identity=next((v for v in c['env'] if v['name']=='CLUSTER_NAME'),{})
            if identity.get('valueFrom',{}).get('configMapKeyRef')!={'name':'devoops-cluster-info','key':'CLUSTER_NAME'}:
                raise ValueError('cluster identity must come from worker ConfigMap')
    trainer=next(c for c in containers if c['name']=='trainer')
    if str(trainer.get('resources',{}).get('limits',{}).get('nvidia.com/gpu'))!='1':
        raise ValueError('expected one GPU per trainer Pod')
    address=name+'-'+jobs[0]['name']+'-0-0.'+name
    replaced=0
    for index,arg in enumerate(trainer.get('args',[])):
        if '--max-restarts=0' not in arg: continue
        trainer['args'][index],count=re.subn(r'--master-addr(?:=|\s+)[^\s;]+','--master-addr='+address,arg)
        replaced+=count
    if replaced!=1: raise ValueError('expected single torchrun master address and --max-restarts=0')
    return result
