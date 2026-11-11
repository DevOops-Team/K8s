import hashlib
import json
import unittest
from recovery.jobsets import render_recovery


def original():
    identity=dict(name='CLUSTER_NAME',valueFrom={'configMapKeyRef':{'name':'devoops-cluster-info','key':'CLUSTER_NAME'}})
    def container(name):
        c=dict(name=name,image='ghcr.io/team/'+name+'@sha256:fixed',env=[{'name':'GENERATION','value':'1'}])
        if name in ('ckpt-init','ckpt-sidecar'): c['env'].append(identity.copy())
        return c
    trainer=container('trainer');trainer['resources']={'limits':{'nvidia.com/gpu':1}};trainer['args']=['exec torchrun --nnodes=4 --master-addr=job-j1-g0001-workers-0-0.job-j1-g0001 --max-restarts=0 train.py']
    return dict(apiVersion='jobset.x-k8s.io/v1alpha2',kind='JobSet',
        metadata=dict(name='job-j1-g0001',namespace='training',labels={'devoops/job-id':'j1','devoops/generation':'1'}),
        spec=dict(failurePolicy={'maxRestarts':0},network={'subdomain':'job-j1-g0001'},replicatedJobs=[dict(name='workers',replicas=1,template=dict(spec=dict(
            completionMode='Indexed',completions=4,parallelism=4,backoffLimit=0,template=dict(metadata={'labels':{'devoops/generation':'1'}},spec=dict(restartPolicy='Never',
            initContainers=[container(n) for n in ('ckpt-init','ckpt-sidecar','progress-reporter')],containers=[trainer])))))]))

class JobsetTests(unittest.TestCase):
    def render(self,obj):
        raw=json.dumps(obj).encode()
        return render_recovery(raw,hashlib.sha256(raw).hexdigest(),'j1',2,'recovery-1',4)
    def test_template_identity_and_images_preserved(self):
        obj=original();out=self.render(obj)
        self.assertEqual(out['metadata']['name'],'job-j1-g0002')
        self.assertEqual(out['metadata']['labels']['kueue.x-k8s.io/queue-name'],'queue-recovery')
        before=obj['spec']['replicatedJobs'][0]['template']['spec']['template']['spec']
        after=out['spec']['replicatedJobs'][0]['template']['spec']['template']['spec']
        self.assertEqual([x['image'] for x in before['initContainers']],[x['image'] for x in after['initContainers']])
        self.assertEqual(out['spec']['replicatedJobs'][0]['template']['spec']['parallelism'],4)
        self.assertIn('--master-addr=job-j1-g0002-workers-0-0.job-j1-g0002',after['containers'][0]['args'][0])
        self.assertEqual(obj['metadata']['name'],'job-j1-g0001')
        for c in after['initContainers']+after['containers']:
            self.assertEqual(next(e['value'] for e in c['env'] if e['name']=='GENERATION'),'2')
            self.assertFalse(any(e['name']=='CHECKPOINT_URI' for e in c['env']))
    def test_wrong_checksum_rejected(self):
        with self.assertRaises(ValueError):render_recovery(b'{}','0'*64,'j1',2,'r1',4)
    def test_auto_restart_rejected(self):
        obj=original();obj['spec']['failurePolicy']['maxRestarts']=3
        with self.assertRaises(ValueError): self.render(obj)
    def test_live_object_rejected(self):
        obj=original();obj['metadata']['uid']='cluster-uid'
        with self.assertRaises(ValueError): self.render(obj)

    def test_gpu_count_mismatch_rejected(self):
        obj=original();obj['spec']['replicatedJobs'][0]['template']['spec']['completions']=3
        with self.assertRaises(ValueError): self.render(obj)
