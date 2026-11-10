"""Recovery controller with observe/execute modes and external execution adapters."""
import argparse
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


def plan(event):
    required = ('event_id', 'cluster')
    if not isinstance(event, dict) or any(not isinstance(event.get(k), str) or not event[k].strip() for k in required):
        raise ValueError('event_id and cluster must be nonempty strings')
    if event.get('job_id') is not None and not isinstance(event['job_id'], str):
        raise ValueError('job_id must be a string or null')
    from init_failure import decision
    init = decision(event)
    if not init and not event.get('suggested_action') and not event.get('fault_type'):
        raise ValueError('suggested_action or legacy fault_type required')
    if event['cluster'] not in ('eks-a', 'eks-b', 'eks-c'):
        raise ValueError('unknown cluster')
    actions = {
        'restart_in_place': 'RETRY_GROUP',
        'isolate_migrate': 'QUARANTINE_AND_RETRY',
        'reset_then_migrate': 'QUARANTINE_AND_RETRY',
        # Pre-save migration is deferred; use the normal generation-first spare path.
        'evacuate_preemptive': 'QUARANTINE_AND_RETRY',
        'fence_evacuate': 'FENCE_AND_EVACUATE',
        'hold': 'HOLD', 'refuse': 'NEEDS_CONFIG_FIX', 'none': 'NONE',
    }
    legacy = {'software_transient': 'restart_in_place',
              'software_permanent': 'refuse', 'node_hardware': 'isolate_migrate',
              'spot_interruption': 'evacuate_preemptive', 'straggler': 'hold',
              'az_failure': 'fence_evacuate'}
    recommendation = event.get('suggested_action', legacy.get(event.get('fault_type'), 'hold'))
    if not init and recommendation not in actions:
        raise ValueError('unknown suggested_action')
    action = init['action'] if init else actions[recommendation]
    if event.get('collector_healthy') is False and event.get('category') != 'site':
        action = 'HOLD'
    return {'event_id': event['event_id'], 'job_id': event.get('job_id'),
            'cluster': event['cluster'], 'suggested_action': action,
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
            import re
            namespace = config.get('job_namespace', 'training')
            if not isinstance(namespace, str) or not re.fullmatch('[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?', namespace):
                raise ValueError('invalid job_namespace')

    from evidence import policy_config
    policy_config(config.get('evidence_policy'))
    watchdog = config.get('watchdog', {})
    if not isinstance(watchdog, dict) or type(watchdog.get('enabled', False)) is not bool:
        raise ValueError('watchdog.enabled must be boolean')
    if watchdog.get('enabled'):
        from urllib.parse import urlparse
        url = urlparse(watchdog.get('url', ''))
        if (source != 'kafka' or url.scheme not in ('http', 'https') or not url.netloc
                or url.username or url.password or url.path != '/api/v2/alerts'
                or url.query or url.fragment):
            raise ValueError('watchdog requires an Alertmanager /api/v2/alerts URL and Kafka')
        timeout = watchdog.get('timeout_seconds', 300)
        poll = watchdog.get('poll_seconds', 30)
        if type(timeout) is not int or type(poll) is not int or not 1 <= poll < timeout:
            raise ValueError('watchdog requires positive poll_seconds < timeout_seconds')
    from retry_policy import DEFAULTS
    policy = config.get('retry_policy', {})
    if not isinstance(policy, dict) or set(policy) - set(DEFAULTS):
        raise ValueError('unknown retry_policy field')
    merged = dict(DEFAULTS, **policy)
    if any(type(v) is not int or v <= 0 for v in merged.values()):
        raise ValueError('retry policy values must be positive integers')
    if merged['api_attempts'] != 3:
        raise ValueError('API policy requires 3 total attempts')
    if merged['kafka_max_backoff_seconds'] < merged['kafka_backoff_seconds']:
        raise ValueError('Kafka maximum backoff must be >= initial backoff')
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
