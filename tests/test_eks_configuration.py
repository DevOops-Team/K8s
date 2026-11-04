import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import yaml
ROOT=Path(__file__).resolve().parents[1]

class EksConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'repo'
        shutil.copytree(ROOT,self.root,ignore=shutil.ignore_patterns('.git','.venv','__pycache__','.build'))
        spec=importlib.util.spec_from_file_location('eks_config',self.root/'scripts/configure-eks.py')
        self.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.module)
        self.settings=yaml.safe_load((self.root/'eks/eks-a/settings.example.yaml').read_text())
    def test_one_cluster_update_and_aggregate_budget(self):
        untouched={n:(self.root/f'eks/{n}/resources.yaml').read_bytes() for n in ['eks-b','eks-c']}
        self.settings.update(cpu_quota=24,memory_quota='96Gi',gpu_quota=3,gpu_node_labels={'gpu-pool':'a'})
        self.module.configure('eks-a',self.settings)
        a=list(yaml.safe_load_all((self.root/'eks/eks-a/resources.yaml').read_text()))
        manager=list(yaml.safe_load_all((self.root/'onprem/scheduling/resources.yaml').read_text()))
        self.assertEqual(self.module.quota(a)['nvidia.com/gpu'],3)
        self.assertEqual(self.module.quota(manager),{'cpu':88,'memory':'352Gi','nvidia.com/gpu':11})
        for n,content in untouched.items():self.assertEqual((self.root/f'eks/{n}/resources.yaml').read_bytes(),content)
    def test_mismatched_profile_and_overquota_do_not_write(self):
        before=(self.root/'eks/eks-a/resources.yaml').read_bytes()
        self.settings['cluster_alias']='eks-b'
        with self.assertRaises(ValueError):self.module.configure('eks-a',self.settings)
        self.settings.update(cluster_alias='eks-a',gpu_quota=5)
        with self.assertRaises(ValueError):self.module.configure('eks-a',self.settings)
        self.assertEqual(before,(self.root/'eks/eks-a/resources.yaml').read_bytes())
    def test_root_destinations_are_isolated(self):
        for alias in ['eks-a','eks-b','eks-c']:
            root=yaml.safe_load((self.root/f'argocd/bootstrap/eks/{alias}/root.yaml').read_text())
            project=yaml.safe_load((self.root/f'argocd/bootstrap/eks/{alias}/project.yaml').read_text())
            self.assertEqual(root['spec']['source']['path'],'argocd/applications/eks/'+alias)
            self.assertTrue(all(d['name']==alias for d in project['spec']['destinations']))
        default=yaml.safe_load((self.root/'argocd/bootstrap/kustomization.yaml').read_text())
        self.assertEqual(default['resources'],['onprem'])
    def test_auto_sync_flag_affects_only_selected_target(self):
        subprocess.run([sys.executable,str(self.root/'scripts/configure.py'),'--repo-url','https://github.com/DevOops-Team/K8s.git','--scope','eks-a','--enable-auto-sync'],check=True,capture_output=True)
        for p in (self.root/'argocd/applications').rglob('gpu-*.yaml'):
            d=yaml.safe_load(p.read_text());enabled='automated' in d['spec']['syncPolicy']
            self.assertEqual(enabled,d['spec']['destination'].get('name')=='eks-a')

if __name__=='__main__':unittest.main()
