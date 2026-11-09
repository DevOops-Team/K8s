"""Recovery controller with observe/execute modes and external execution adapters."""
import argparse
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


def plan(event):
    required = ('event_id', 'job_id', 'cluster', 'fault_type')
    if not isinstance(event, dict) or any(not isinstance(event.get(k), str) or not event[k].strip() for k in required):
        raise ValueError('event_id, job_id, cluster, fault_type must be nonempty strings')
    if event['cluster'] not in ('eks-a', 'eks-b', 'eks-c'):
        raise ValueError('unknown cluster')
    actions = {'software_transient': 'RESTART_SAME_CLUSTER',
               'software_permanent': 'REJECT', 'node_hardware': 'RELOCATE_SAME_CLUSTER',
               'spot_interruption': 'RELOCATE_SAME_CLUSTER',
               'straggler': 'REVIEW', 'az_failure': 'HOLD_FOR_FENCING'}
    return {'event_id': event['event_id'], 'job_id': event['job_id'],
            'cluster': event['cluster'], 'suggested_action': actions.get(event['fault_type'], 'REVIEW'),
            'mode': 'observe', 'executed': False}


def validate_config(config):
    if not isinstance(config, dict) or config.get('schema_version') != 1:
        raise ValueError('schema_version must be 1')
    source=config.get('event_source')
    if type(config.get('execution_enabled')) is not bool:
        raise ValueError('execution_enabled must be boolean')
    if source not in ('disabled','kafka') or config.get('mode') not in ('observe','execute'):
        raise ValueError('unsupported mode/source')
    if (config['mode']=='execute') != config['execution_enabled']:
        raise ValueError('execution_enabled must match execute mode')
    if source == 'disabled' and config['execution_enabled']:
        raise ValueError('execution requires kafka input')
    if source == 'kafka':
        for key in ('aws_region','recovery_table','fencing_table','kafka_bootstrap_servers','kafka_group_id','fault_topic','result_topic'):
            if not isinstance(config.get(key),str) or not config[key].strip() or 'YOUR_' in config[key]:
                raise ValueError('configure '+key)
        if config['recovery_table']==config['fencing_table']:
            raise ValueError('recovery inbox and job_id fencing table must be separate')
        if config['fault_topic']==config['result_topic']:
            raise ValueError('input and output topics must differ')
        if type(config.get('reconcile_seconds',5)) is not int or not 1 <= config.get('reconcile_seconds',5) <= 60:
            raise ValueError('reconcile_seconds must be 1..60')
        if config['execution_enabled']:
            if config.get('redispatch_policy')!='controller-only' or config.get('scheduler_policy_confirmed') is not True:
                raise ValueError('A policy must be configured and reviewed before execution')
            from urllib.parse import urlparse
            if urlparse(config.get('executor_url','')).scheme != 'https':
                raise ValueError('HTTPS executor_url required')
    return config


HEALTH = {"kafka_connected": False, "worker_ok": False, "execution_enabled": False, "event_source":"disabled"}


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/healthz', '/readyz'):
            self.send_error(404)
            return
        body = json.dumps(HEALTH).encode()
        ready = (HEALTH['event_source']=='disabled'
                 or (HEALTH.get('lease_connected') and not HEALTH.get('leader'))
                 or (HEALTH['worker_ok'] and HEALTH['kafka_connected']))
        self.send_response(200 if self.path=='/healthz' or ready else 503)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--event-file', type=Path)
    parser.add_argument('--config', type=Path, default=Path('/etc/recovery/config.json'))
    args = parser.parse_args()
    if args.event_file:
        print(json.dumps(plan(json.loads(args.event_file.read_text()))))
        return
    config=validate_config(json.loads(args.config.read_text()))
    stop=threading.Event()
    HEALTH.update(event_source=config['event_source'], execution_enabled=config['execution_enabled'])
    server=HTTPServer(('0.0.0.0',8080),Health)
    thread=threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    def terminate(*_):
        stop.set()
    signal.signal(signal.SIGTERM,terminate)
    signal.signal(signal.SIGINT,terminate)
    try:
        if config['event_source']=='kafka':
            from runtime import run
            from os import environ
            if environ.get('RECOVERY_POD_UID'):
                from leadership import run_elected
                run_elected(run, config, stop, HEALTH, environ['RECOVERY_POD_UID'])
            else:
                run(config,stop,HEALTH)
        else:
            stop.wait()
    finally:
        stop.set()
        server.shutdown()
        server.server_close()



if __name__ == '__main__':
    main()
