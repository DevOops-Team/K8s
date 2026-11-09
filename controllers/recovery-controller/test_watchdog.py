import copy
import unittest
from watchdog import Watchdog, watchdog_active


class Store:
    def __init__(self):
        self.state = {}
        self.notices = set()
    def watchdog_state(self):
        return copy.deepcopy(self.state)
    def watchdog_save(self, **values):
        self.state.update(values)
    def watchdog_notice(self, rid):
        self.notices.add(rid)


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.alerts = []
        self.guard = Watchdog(self.store, 'http://monitor/api/v2/alerts',
                              fetch=lambda: self.alerts)
    def test_initial_grace_then_hold_without_restart(self):
        self.assertFalse(self.guard.check(1000))
        self.assertFalse(self.guard.check(1299))
        self.assertTrue(self.guard.check(1330))
        self.assertEqual(len(self.store.notices), 1)
        self.guard.check(1400)
        self.assertEqual(len(self.store.notices), 1)
    def test_fresh_fault_suppresses_joint_silence(self):
        self.guard.check(1000)
        self.guard.fault_received(1280)
        self.assertFalse(self.guard.check(1330))
        self.assertTrue(self.guard.check(1600))
    def test_healthy_watchdog_clears_hold(self):
        self.guard.check(1000)
        self.assertTrue(self.guard.check(1300))
        self.alerts = [dict(labels={'alertname':'Watchdog'}, status={'state':'active'},
                            endsAt='2099-01-01T00:00:00Z', updatedAt='1970-01-01T00:22:10Z')]
        self.assertFalse(self.guard.check(1330))
    def test_handover_preserves_silence_and_deduplicates_notice(self):
        self.guard.check(1000)
        other = Watchdog(self.store, 'http://monitor/api/v2/alerts', fetch=lambda: [])
        self.assertTrue(other.check(1400))
        self.guard.check(1430)
        self.assertEqual(len(self.store.notices), 1)
    def test_expired_or_resolved_is_not_healthy(self):
        alert=dict(labels={'alertname':'Watchdog'},status={'state':'active'},
                   endsAt='1970-01-01T00:00:01Z', updatedAt='1970-01-01T00:00:00Z')
        self.assertFalse(watchdog_active([alert], 1000))
        alert.update(endsAt='2099-01-01T00:00:00Z',status={'state':'suppressed'})
        self.assertFalse(watchdog_active([alert], 1000))
    def test_unreachable_endpoint_counts_as_silence(self):
        def unavailable():
            raise OSError('unreachable')
        guard=Watchdog(self.store,'http://monitor/api/v2/alerts',fetch=unavailable)
        self.assertFalse(guard.check(1000))
        self.assertTrue(guard.check(1300))

    def test_cached_firing_alert_is_not_a_new_heartbeat(self):
        self.alerts = [dict(labels={'alertname':'Watchdog'},status={'state':'active'},
                            updatedAt='1970-01-01T00:16:40Z', endsAt='2099-01-01T00:00:00Z')]
        self.assertFalse(self.guard.check(1000))
        self.assertTrue(self.guard.check(1300))
