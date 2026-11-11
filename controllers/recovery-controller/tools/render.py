#!/usr/bin/env python3
"""Render a recovery controller deployment to stdout without contacting an API."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import yaml
from main import validate_config

DEFAULT_MANIFEST = Path(__file__).resolve().parents[4] / 'gpu-gitops/onprem/controllers/recovery-controller/resources.yaml'

def render(image, config, manifest=None):
    if not re.fullmatch(r'ghcr\.io/[a-z0-9._/-]+@sha256:[a-f0-9]{64}', image) or image.endswith('0'*64):
        raise ValueError('Use an actual GHCR image pinned by sha256 digest')
    validate_config(config)
    manifest = Path(manifest or os.environ.get('RECOVERY_DEPLOYMENT_MANIFEST', DEFAULT_MANIFEST))
    docs = list(yaml.safe_load_all(manifest.read_text()))
    for document in docs:
        if document['metadata'].get('name') == 'recovery-controller-relocation':
            document['metadata']['namespace'] = config.get('job_namespace', 'training')
    cm=next(d for d in docs if d['kind']=='ConfigMap')
    cm['data']['config.json']=json.dumps(config,sort_keys=True)
    deployment=next(d for d in docs if d['kind']=='Deployment')
    deployment['spec']['replicas']=3
    pod=deployment['spec']['template']
    pod['metadata']['annotations']={'checksum/config':hashlib.sha256(cm['data']['config.json'].encode()).hexdigest()}
    pod['spec']['containers'][0]['image']=image
    pod['spec']['automountServiceAccountToken']=config['event_source']=='kafka'
    if config['event_source']=='kafka':
        pod['spec']['containers'][0]['envFrom']=[{'secretRef':{'name':'recovery-controller-credentials'}}]
    return docs

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--image',required=True)
    p.add_argument('--config',type=Path,default=Path(__file__).resolve().parents[1] / 'examples' / 'config.example.json')
    p.add_argument('--manifest', type=Path, help='Path to the Gitops recovery-controller resources.yaml')
    args=p.parse_args()
    try:
        print(yaml.safe_dump_all(render(args.image,json.loads(args.config.read_text()), args.manifest),sort_keys=False),end='')
    except (ValueError,OSError) as e:
        p.exit(1,str(e)+'\n')
