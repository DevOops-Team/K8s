"""Capacity/load assumptions for the temporary GPU-164 profile and full-size return."""
import importlib.util
from pathlib import Path
import sys
import unittest

spec = importlib.util.spec_from_file_location('e2e_topology', Path(__file__).parents[1] / 'e2e/harness/topology.py')
T = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = T
spec.loader.exec_module(T)


class Profiles(unittest.TestCase):
    def test_load_really_saturates_each_profile_and_respects_routing(self):
        for name, p in T.PROFILES.items():
            with self.subTest(name=name):
                self.assertEqual(p.heavy + p.light, p.training)
                self.assertEqual(p.recovery, 4)
                jobs = p.load_jobs()
                self.assertEqual(sum(g for _, g, _, _ in jobs), p.training)
                for w in p.workers:
                    self.assertEqual(sum(g for _, g, _, worker in jobs if worker == w), 4)
                self.assertEqual(jobs[0], ('train-a', 4, 'queue-heavy', p.workers[0]))
                self.assertTrue(all(g <= 2 and q == 'queue-light' for _, g, q, _ in jobs[1:]))

    def test_gpu10_and_full_are_explicitly_reversible(self):
        self.assertEqual((T.profile('gpu10').heavy, T.profile('gpu10').light), (6, 2))
        self.assertEqual((T.profile('full').heavy, T.profile('full').light), (8, 4))
        self.assertEqual(len(T.profile('gpu10').workers) * 5, 10)

    def test_invalid_profile_is_rejected(self):
        with self.assertRaises(ValueError):
            T.profile('typo')
