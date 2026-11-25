"""GPU-248: classifier events carry floats; boto3 only accepts Decimal."""
import math
import unittest
from decimal import Decimal
from unittest.mock import Mock

from integrations.dynamodb import Store


class StoreFloatTests(unittest.TestCase):
    def setUp(self):
        self.table = Mock()
        self.store = Store(self.table, Mock(), clock=lambda: 0)

    def test_float_fields_are_stored_as_decimal(self):
        event = {'event_id': 'e1', 'confidence': 0.93, 'metrics': {'temp_c': 88.5, 'xid': [79, 0.5]}}
        self.store.accept(event, 'd')
        stored = self.table.put_item.call_args.kwargs['Item']['event']
        self.assertEqual(stored['confidence'], Decimal('0.93'))
        self.assertEqual(stored['metrics'], {'temp_c': Decimal('88.5'), 'xid': [79, Decimal('0.5')]})
        self.assertEqual(event['confidence'], 0.93)  # caller's event (digest input) unchanged

    def test_non_finite_float_is_rejected(self):
        for value in (math.nan, math.inf):
            with self.assertRaises(ValueError):
                self.store.accept({'event_id': 'e2', 'score': value}, 'd')
        self.table.put_item.assert_not_called()

    def test_dynamodb_integer_generation_can_build_relocation_request(self):
        from integrations.relocation import RelocationClient
        event = dict(event_id='r1', job_id='j1', generation=Decimal('1'))
        record = dict(kind='event', event=event)
        self.table.get_item.return_value = {'Item': record}
        self.table.update_item.return_value = {'Attributes': record}
        self.table.scan.return_value = {'Items': [record]}
        transport = Mock()
        client = RelocationClient('training', transport)
        for loaded in (self.store.get('r1'), self.store.save('r1', api_attempts=1),
                       next(self.store.records())):
            client.submit(loaded['event'], 'restartInPlace')
            self.assertEqual(transport.call_args.args[2]['spec']['jobRef'], 'training/job-j1-g0001')
        self.assertIsInstance(event['generation'], Decimal)
