"""Single-replica Kafka inbox + restartable workflow worker."""
import json
import os
import threading
import time
from adapters import Store, Executor
from workflow import Workflow, TERMINAL


def accept_message(consumer, workflow, msg):
    event=json.loads(msg.value())
    workflow.accept(event)
    consumer.commit(message=msg, asynchronous=False)


def run(config, stop, health):
    import boto3
    from confluent_kafka import Consumer, Producer
    dynamodb = boto3.resource('dynamodb', region_name=config['aws_region'])
    table = dynamodb.Table(config['recovery_table'])
    store = Store(table, dynamodb.Table(config['fencing_table']))
    execute = config['execution_enabled']
    executor = Executor(config['executor_url'], os.environ.get('EXECUTOR_TOKEN','')) if execute else None
    workflow = Workflow(store, executor, execute)
    security = json.loads(os.environ.get('KAFKA_CLIENT_CONFIG_JSON','{}'))
    base = dict(security, **{'bootstrap.servers':config['kafka_bootstrap_servers']})
    consumer = Consumer(dict(base, **{'group.id':config['kafka_group_id'],
        'enable.auto.commit':False, 'enable.auto.offset.store':False,
        'auto.offset.reset':'earliest'}))
    producer = Producer(base)

    def worker():
        while not stop.is_set():
            try:
                for record in store.records():
                    if stop.is_set(): break
                    try:
                        if record['state'] not in TERMINAL:
                            record = workflow.tick(record)
                        if record['state'] == 'RECOVERED':
                            store.finish(record)
                        if record['state'] in TERMINAL and not record.get('published'):
                            errors=[]
                            payload={'event_id':record['event']['event_id']+':result',
                                     'job_id':record['event']['job_id'], 'state':record['state'],
                                     'generation':int(record.get('generation',record['event']['generation']))}
                            producer.produce(config['result_topic'],key=payload['job_id'],
                                value=json.dumps(payload),on_delivery=lambda err,msg: errors.append(err))
                            if producer.flush(10) or not errors or errors[0]:
                                raise RuntimeError('result event delivery unconfirmed')
                            store.save(record['event']['event_id'],published=True)
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
            health['kafka_connected']=True
    finally:
        stop.set()
        consumer.close()
        thread.join(timeout=15)
