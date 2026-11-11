"""Kafka clients and durable event intake."""
import json
import os


def accept_message(consumer, workflow, msg):
    envelope=json.loads(msg.value())
    event=envelope.get('payload', envelope)
    headers = dict(msg.headers() or []) if hasattr(msg, 'headers') else {}
    if headers.get('rule_version'):
        event = dict(event, rule_version=headers['rule_version'].decode('utf-8'))
    workflow.accept(event)
    consumer.commit(message=msg, asynchronous=False)


def create_clients(config):
    from confluent_kafka import Consumer, Producer
    security = json.loads(os.environ.get('KAFKA_CLIENT_CONFIG_JSON','{}'))
    base = dict(security, **{'bootstrap.servers':config['kafka_bootstrap_servers']})
    consumer = Consumer(dict(base, **{'group.id':config['kafka_group_id'],
        'enable.auto.commit':False, 'enable.auto.offset.store':False,
        'auto.offset.reset':'earliest'}))
    producer = Producer(base)
    return consumer, producer
