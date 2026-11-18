"""Kafka clients and durable event intake."""
import json
import os


JOB_LIFECYCLE_TOPIC = 'job.lifecycle'


def lifecycle_failure(envelope):
    if envelope.get('schema_version') != 1:
        raise ValueError('job.init_failed requires schema_version 1')
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
    rank = payload.get('rank')
    if rank is not None:
        if type(rank) is not int or rank < 0:
            raise ValueError('job.init_failed rank must be a nonnegative integer')
        event['rank'] = rank
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
        if envelope.get('type') not in ('job.init_failed', 'job.failed'):
            consumer.commit(message=msg, asynchronous=False)
            return False
        if envelope['type'] == 'job.failed':
            if type(envelope.get('schema_version')) is not int or envelope['schema_version'] != 1:
                raise ValueError('job.failed requires schema_version 1')
            event = {key: envelope.get(key) for key in
                     ('event_id', 'job_id', 'generation', 'cluster', 'node')}
            if not event['job_id'] or type(event['generation']) is not int or event['generation'] < 1:
                raise ValueError('job.failed requires job_id and generation')
            event.update(component='job-watcher', lifecycle_type='job.failed',
                         suggested_action='hold', category='app')
        else:
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
