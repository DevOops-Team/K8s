"""Kafka clients and durable event intake."""
import json
import os


JOB_LIFECYCLE_TOPIC = 'job.lifecycle'


def lifecycle_failure(envelope):
    payload = envelope.get('payload')
    if not isinstance(payload, dict):
        raise ValueError('job.init_failed requires a payload')
    event = {key: envelope[key] for key in ('event_id', 'job_id', 'generation', 'cluster', 'node')
             if key in envelope}
    for key in ('job_id', 'generation', 'cluster', 'node'):
        if key in payload:
            if key in event and event[key] != payload[key]:
                raise ValueError(f'job.init_failed conflicting {key}')
            event[key] = payload[key]
    event.update(component='ckpt-init', category='app',
                 exit_code=payload.get('exit_code'), pod_uid=payload.get('pod_uid'),
                 jobset_uid=payload.get('jobset_uid'))
    reason = payload.get('reason')
    if isinstance(reason, str):
        event['init_reason'] = reason
    gen, step = payload.get('checkpoint_gen'), payload.get('checkpoint_step')
    if type(gen) is int and type(step) is int:
        event['checkpoint'] = {'generation': gen, 'step': step}
    return event


def accept_message(consumer, workflow, msg):
    envelope=json.loads(msg.value())
    lifecycle = msg.topic() == JOB_LIFECYCLE_TOPIC if hasattr(msg, 'topic') else False
    if lifecycle:
        if envelope.get('type') != 'job.init_failed':
            consumer.commit(message=msg, asynchronous=False)
            return False
        event = lifecycle_failure(envelope)
    else:
        event = envelope.get('payload', envelope)
    headers = dict(msg.headers() or []) if hasattr(msg, 'headers') else {}
    if headers.get('rule_version'):
        event = dict(event, rule_version=headers['rule_version'].decode('utf-8'))
    workflow.accept(event)
    consumer.commit(message=msg, asynchronous=False)
    return not lifecycle


def create_clients(config):
    from confluent_kafka import Consumer, Producer
    security = json.loads(os.environ.get('KAFKA_CLIENT_CONFIG_JSON','{}'))
    base = dict(security, **{'bootstrap.servers':config['kafka_bootstrap_servers']})
    consumer = Consumer(dict(base, **{'group.id':config['kafka_group_id'],
        'enable.auto.commit':False, 'enable.auto.offset.store':False,
        'auto.offset.reset':'earliest'}))
    producer = Producer(base)
    return consumer, producer
