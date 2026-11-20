import unittest
from unittest.mock import Mock
from runtime.heartbeat_metrics import HeartbeatMetrics


class HeartbeatMetricsTests(unittest.TestCase):
    def setUp(self):
        self.now = 1735689720
        self.metrics = HeartbeatMetrics(clock=lambda: self.now)
        self.table = Mock()
        self.item = dict(job_id='j1', generation=1, heartbeat_at='2025-01-01T00:00:00Z')

    def test_paginated_snapshot(self):
        self.table.scan.side_effect = [dict(Items=[self.item], LastEvaluatedKey={'job_id':'j1'}),
                                       dict(Items=[dict(self.item, job_id='j2')])]
        self.metrics.refresh(self.table)
        self.assertEqual(self.now - self.metrics.rows[('j1', '1')], 120)
        self.assertIn(('j2', '1'), self.metrics.rows)
        self.assertEqual(self.table.scan.call_args.kwargs['ExclusiveStartKey'], {'job_id':'j1'})
        self.assertEqual(self.metrics.success, 1)

    def test_failed_page_discards_partial_and_old_snapshot(self):
        self.table.scan.return_value = dict(Items=[self.item])
        self.metrics.refresh(self.table)
        self.table.scan.side_effect = [dict(Items=[self.item], LastEvaluatedKey={'job_id':'j1'}),
                                       OSError('AWS unavailable')]
        self.metrics.refresh(self.table)
        self.assertEqual(self.metrics.rows, {})
        self.assertEqual(self.metrics.success, 0)
        self.assertEqual(self.metrics.errors, 1)

    def test_invalid_missing_and_non_utc_values(self):
        values = [dict(self.item, heartbeat_at=v) for v in (None, 'bad', '2025-01-01T00:00:00',
                                                         '2025-01-01T01:00:00+01:00')]
        values += [dict(self.item, generation=True), dict(self.item, generation=1.5)]
        self.table.scan.return_value = dict(Items=values)
        self.metrics.refresh(self.table)
        self.assertEqual(self.metrics.invalid, 6)
        self.assertEqual(self.metrics.rows, {})

    def test_generation_change_and_deleted_items_remove_old_labels(self):
        self.table.scan.return_value = dict(Items=[self.item])
        self.metrics.refresh(self.table)
        self.table.scan.return_value = dict(Items=[dict(self.item, generation=2)])
        self.metrics.refresh(self.table)
        self.assertEqual(set(self.metrics.rows), {('j1', '2')})
        self.table.scan.return_value = dict(Items=[])
        self.metrics.refresh(self.table)
        self.assertEqual(self.metrics.rows, {})

    def test_future_timestamp_remains_available_for_scrape_clamping(self):
        self.table.scan.return_value = dict(Items=[dict(self.item, heartbeat_at='2025-01-01T00:03:00+00:00')])
        self.metrics.refresh(self.table)
        self.assertGreater(self.metrics.rows[('j1','1')], self.now)

    def test_exposition_dynamic_age_failure_and_stale_snapshot(self):
        from prometheus_client import CollectorRegistry, generate_latest
        registry = CollectorRegistry()
        registry.register(self.metrics)
        output = lambda: generate_latest(registry).decode()
        self.assertIn('recovery_fencing_heartbeat_scrape_success 0.0', output())
        self.table.scan.return_value = dict(Items=[self.item])
        self.metrics.refresh(self.table)
        self.assertIn('generation="1",job_id="j1"} 120.0', output())
        self.now += 5
        self.assertIn('generation="1",job_id="j1"} 125.0', output())
        self.assertEqual(self.table.scan.call_count, 1)
        self.now += 61
        self.assertNotIn('job_id="j1"', output())
        self.assertIn('recovery_fencing_heartbeat_scrape_success 0.0', output())
        self.metrics.failed()
        self.assertIn('recovery_fencing_heartbeat_read_errors_total 1.0', output())


class HeartbeatRunRecoveryTests(unittest.TestCase):
    """GPU-248: a failed scan must rebuild the client instead of reusing it until restart."""

    def test_failed_refresh_rebuilds_table_client(self):
        import threading
        metrics = HeartbeatMetrics()
        metrics.interval_seconds = 0
        stop = threading.Event()
        broken, healthy = Mock(), Mock()
        broken.scan.side_effect = OSError('no credentials')

        def healthy_scan(**_):
            stop.set()
            return dict(Items=[])
        healthy.scan.side_effect = healthy_scan
        tables = iter([broken, healthy])
        metrics.run(dict(aws_region='us-east-2', fencing_table='t'), stop,
                    table_factory=lambda config: next(tables))
        self.assertEqual(metrics.errors, 1)
        self.assertEqual(metrics.success, 1)
        self.assertEqual(broken.scan.call_count, 1)
