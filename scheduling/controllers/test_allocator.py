"""python -m unittest -v test_allocator.py  (쿠버네티스 없이 돈다)"""
import unittest

from allocation_rules import aged, want_tight


class Aging(unittest.TestCase):
    def test_gpu_weighted(self):
        # 기준 720 GPU·초 = 12 GPU·분
        self.assertTrue(aged(180, 4, 720))     # 4장 3분
        self.assertFalse(aged(179, 4, 720))
        self.assertFalse(aged(700, 1, 720))    # 1장은 12분 기다려야
        self.assertTrue(aged(720, 1, 720))
        self.assertTrue(aged(360, 2, 720))     # 2장 6분

    def test_zero_gpu_counts_as_one(self):
        self.assertTrue(aged(720, 0, 720))


class Borrow(unittest.TestCase):
    T, R = 60, 300

    def w(self, tight, wait, idle):
        return want_tight(tight, wait, idle, self.T, self.R)

    def test_tighten_only_after_wait(self):
        self.assertFalse(self.w(False, 30, None))   # 막 들어온 Heavy 잡: 아직
        self.assertTrue(self.w(False, 60, None))

    def test_stay_tight_while_heavy_waits(self):
        self.assertTrue(self.w(True, 5, None))      # 새 Heavy 잡이 막 들어와도 유지

    def test_release_after_idle(self):
        self.assertTrue(self.w(True, None, 0))
        self.assertTrue(self.w(True, None, 299))
        self.assertFalse(self.w(True, None, 300))

    def test_loose_stays_loose_without_heavy(self):
        self.assertFalse(self.w(False, None, 0))
        self.assertFalse(self.w(False, None, 10_000))


if __name__ == "__main__":
    unittest.main()
