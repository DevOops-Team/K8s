"""Single-replica Kafka inbox + restartable workflow worker."""
import json
import os
import threading
import time
from adapters import Store
from relocation import RelocationClient
from workflow import Workflow, TERMINAL
from watchdog import Watchdog
from retry_policy import Runner


def accept_message(consumer, workflow, msg):
    envelope=json.loads(msg.value())
    event=envelope.get('payload', envelope)
    headers = dict(msg.headers() or []) if hasattr(msg, 'headers') else {}
    if headers.get('rule_version'):
        event = dict(event, rule_version=headers['rule_version'].decode('utf-8'))
    workflow.accept(event)
    consumer.commit(message=msg, asynchronous=False)


def run(config, stop, health):
    import boto3
    from botocore.config import Config
    from confluent_kafka import Consumer, Producer
    dynamodb = boto3.resource('dynamodb', region_name=config['aws_region'],
                                config=Config(
                                    retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=10))
    table = dynamodb.Table(config['recovery_table'])
    store = Store(table, dynamodb.Table(config['fencing_table']),
                  max_recovery_attempts=config.get('retry_policy', {}).get('max_recovery_attempts', 3))
    execute = config['execution_enabled']
    executor = RelocationClient(config.get('job_namespace', 'training')) if execute else None
    workflow = Workflow(store, executor, execute, evidence_policy=config.get('evidence_policy'))
    watchdog_config = config.get('watchdog', {})
    guard = Watchdog(store, watchdog_config['url'],
                     watchdog_config.get('timeout_seconds', 300),
                     watchdog_config.get('poll_seconds', 30)) if watchdog_config.get('enabled') else None
    guard_lock = threading.Lock()
    security = json.loads(os.environ.get('KAFKA_CLIENT_CONFIG_JSON','{}'))
    base = dict(security, **{'bootstrap.servers':config['kafka_bootstrap_servers']})
    consumer = Consumer(dict(base, **{'group.id':config['kafka_group_id'],
        'enable.auto.commit':False, 'enable.auto.offset.store':False,
        'auto.offset.reset':'earliest'}))
    producer = Producer(base)
    runner = Runner(store, workflow, producer, config['result_topic'], config.get('retry_policy'))

    def worker():
        while not stop.is_set():
            try:
                with guard_lock:
                    monitoring_hold = guard.check(int(time.time())) if guard else False
                health['monitoring_hold'] = monitoring_hold
                for record in store.records():
                    if stop.is_set(): break
                    try:
                        record = runner.operation(record, int(time.time()), monitoring_hold)
                        runner.deliver(record, int(time.time()))
                    except Exception as exc:
                        # Preserve current stage. Next scan retries with the same idempotency key.
                        print(json.dumps({'event_id':record['event']['event_id'],'error_type':type(exc).__name__}),flush=True)
                health['worker_ok']=True
            except Exception as exc:
                health['worker_ok']=False
                print(json.dumps({'component':'worker','error_type':type(exc).__name__}),flush=True)
            stop.wait(config.get('reconcile_seconds',5))
    thread=threading.Thread(target=worker,daemon=True)
    thread.start()
    consumer.list_topics(topic=config['fault_topic'],timeout=10)
    health['kafka_connected']=True
    consumer.subscribe([config['fault_topic']])
    try:
        while not stop.is_set():
            msg=consumer.poll(1)
            if msg is None: continue
            if msg.error(): raise RuntimeError('Kafka poll failed')
            # Invalid input is not committed: fail closed until schema/source is corrected.
            accept_message(consumer,workflow,msg)  # Durable inbox before offset commit.
            with guard_lock:
                if guard:
                    guard.fault_received(int(time.time()))
            health['kafka_connected']=True
    finally:
        stop.set()
        consumer.close()
        thread.join(timeout=15)
