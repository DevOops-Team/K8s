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
