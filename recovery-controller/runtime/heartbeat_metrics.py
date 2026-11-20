"""Read-only, paginated fencing heartbeat snapshot; scrapes never call AWS."""
import threading
import time
from datetime import datetime
from decimal import Decimal



class HeartbeatMetrics:
    interval_seconds = 30

    def __init__(self, clock=time.time):
        self.clock = clock
        self.lock = threading.Lock()
        self.rows = {}
        self.success = 0
        self.errors = 0
        self.invalid = 0
        self.last_success = 0

    def refresh(self, table):
        rows, invalid = {}, 0
        args = dict(ConsistentRead=True,
                    ProjectionExpression='job_id, generation, heartbeat_at')
        try:
            while True:
                page = table.scan(**args)
                for item in page.get('Items', []):
                    try:
                        job, generation = item['job_id'], item['generation']
                        if not isinstance(job, str) or not job:
                            raise ValueError('invalid job')
                        if (isinstance(generation, bool)
                                or not isinstance(generation, (int, Decimal))
                                or generation < 1 or int(generation) != generation):
                            raise ValueError('invalid generation')
                        value = item['heartbeat_at']
                        if not isinstance(value, str):
                            raise ValueError('invalid timestamp')
                        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
                            raise ValueError('UTC required')
                        rows[(job, str(int(generation)))] = stamp.timestamp()
                    except (KeyError, ValueError, TypeError, OverflowError):
                        invalid += 1
                key = page.get('LastEvaluatedKey')
                if not key:
                    break
                args['ExclusiveStartKey'] = key
        except Exception:
            self.failed()
            return
        with self.lock:
            self.rows, self.invalid, self.success = rows, invalid, 1
            self.last_success = self.clock()

    def failed(self):
        with self.lock:
            self.rows, self.success = {}, 0
            self.errors += 1

    def collect(self):
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
        with self.lock:
            rows = dict(self.rows)
            success, errors, invalid = self.success, self.errors, self.invalid
            last = self.last_success
        now = self.clock()
        fresh = success and 0 <= now - last <= 2 * self.interval_seconds
        age = GaugeMetricFamily('recovery_fencing_heartbeat_age_seconds',
                                'Seconds since fencing heartbeat; absent when snapshot unavailable',
                                labels=['job_id', 'generation'])
        if fresh:
            for labels, stamp in sorted(rows.items()):
                age.add_metric(list(labels), max(0, now - stamp))
        yield age
        yield GaugeMetricFamily('recovery_fencing_heartbeat_scrape_success',
                                'Latest fencing read succeeded and snapshot is fresh', value=int(bool(fresh)))
        yield CounterMetricFamily('recovery_fencing_heartbeat_read_errors',
                                  'Failed fencing table refreshes', value=errors)
        yield GaugeMetricFamily('recovery_fencing_heartbeat_invalid_items',
                                'Items missing valid identity or UTC heartbeat in latest successful refresh',
                                value=invalid)
        yield GaugeMetricFamily('recovery_fencing_heartbeat_last_success_timestamp_seconds',
                                'Unix time of last complete successful fencing refresh', value=last)

    def run(self, config, stop):
        while not stop.is_set():
            try:
                import boto3
                from botocore.config import Config
                db = boto3.resource('dynamodb', region_name=config['aws_region'],
                                    config=Config(retries={'total_max_attempts': 1},
                                                  connect_timeout=5, read_timeout=10))
                table = db.Table(config['fencing_table'])
                while not stop.is_set():
                    self.refresh(table)
                    stop.wait(self.interval_seconds)
            except Exception:
                self.failed()
                stop.wait(self.interval_seconds)
