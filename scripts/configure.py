#!/usr/bin/env python3
"""Edit reviewed Git manifests locally. Never calls Kubernetes or GitHub."""
import argparse
from pathlib import Path
import re
import yaml

ROOT = Path(__file__).resolve().parents[1]
PLACEHOLDER = 'https://github.com/YOUR_ORG/gpu-k8s.git'

def save(path, data):
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=110))

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo-url', required=True)
    p.add_argument('--revision', default='develop')
    p.add_argument('--frontend-image', help='ghcr.io/org/image@sha256:<64 lowercase hex>')
    p.add_argument('--backend-image', help='ghcr.io/org/image@sha256:<64 lowercase hex>')
    p.add_argument('--backend-port', type=int)
    p.add_argument('--acme-email')
    p.add_argument('--scope', choices=['onprem','eks-a','eks-b','eks-c'], default='onprem', help='Only this target has auto-sync enabled; repo URL changes remain repository-wide')
    p.add_argument('--enable-auto-sync', action='store_true', help='After controller/Secret prerequisites are ready')
    args = p.parse_args()
    if not re.match(r'^(https://|ssh://|git@)', args.repo_url) or 'YOUR_ORG' in args.repo_url:
        p.error('Use the actual gpu-k8s repository URL')
    for name in ['frontend','backend']:
        value = getattr(args, name + '_image')
        if value and not re.fullmatch(r'ghcr\.io/[a-z0-9][a-z0-9._/-]*@sha256:[a-f0-9]{64}', value):
            p.error(name + ' image must use an immutable GHCR digest')
    if args.backend_port is not None and not 1 <= args.backend_port <= 65535:
        p.error('backend-port must be 1..65535')
    if args.acme_email and ('@' not in args.acme_email or args.acme_email.endswith('@example.com')):
        p.error('Use an actual ACME contact email')
    root_path = ROOT / 'argocd/bootstrap/onprem/root.yaml'
    old_repo = yaml.safe_load(root_path.read_text())['spec']['source']['repoURL']
    for path in (ROOT/'argocd').rglob('*.yaml'):
        docs = list(yaml.safe_load_all(path.read_text()))
        for doc in docs:
            if not doc:
                continue
            if doc.get('kind') == 'AppProject':
                doc['spec']['sourceRepos'] = [args.repo_url if url in [old_repo,PLACEHOLDER] else url for url in doc['spec']['sourceRepos']]
            if doc.get('kind') == 'Application':
                spec = doc['spec']
                sources = spec.get('sources', [spec.get('source', {})])
                for source in sources:
                    if source.get('repoURL') in [old_repo,PLACEHOLDER]:
                        source['repoURL'] = args.repo_url
                        source['targetRevision'] = args.revision
                target = spec['destination'].get('name','onprem')
                if args.enable_auto_sync and target == args.scope and not doc['metadata']['name'].startswith('gpu-root'):
                    spec['syncPolicy']['automated'] = {'prune': False, 'selfHeal': True, 'allowEmpty': False}
        path.write_text('---\n'.join(yaml.safe_dump(d,sort_keys=False,width=110) for d in docs if d))
    for name in ['frontend','backend']:
        image = getattr(args, name + '_image')
        if not image:
            continue
        repository,digest = image.split('@',1)
        path = ROOT/f'onprem/apps/{name}/overlays/onprem/kustomization.yaml'
        data = yaml.safe_load(path.read_text())
        data['images'][0].update(newName=repository,digest=digest)
        deployment_path = ROOT/f'onprem/apps/{name}/base/deployment.yaml'
        deployment = yaml.safe_load(deployment_path.read_text())
        data['replicas'][0]['count'] = deployment['spec']['replicas']
        save(path,data)
    if args.backend_port:
        for kind in ['deployment','service']:
            path = ROOT/f'onprem/apps/backend/base/{kind}.yaml'
            data = yaml.safe_load(path.read_text())
            if kind == 'deployment':
                data['spec']['template']['spec']['containers'][0]['ports'][0]['containerPort'] = args.backend_port
            else:
                data['spec']['ports'][0]['port'] = args.backend_port
            save(path,data)
    if args.acme_email:
        path = ROOT/'onprem/platform/web/resources.yaml'
        docs = list(yaml.safe_load_all(path.read_text()))
        for d in docs:
            if d and d.get('kind') == 'ClusterIssuer':
                d['spec']['acme']['email'] = args.acme_email
        path.write_text('---\n'.join(yaml.safe_dump(d,sort_keys=False,width=110) for d in docs if d))
    print('Updated local manifests only. Review git diff, validate --ready, then commit/push.')

if __name__ == '__main__':
    main()
