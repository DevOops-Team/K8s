"""python -m unittest -v test_allocator.py  (쿠버네티스 없이 돈다)"""
import unittest

from allocation_rules import aged


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


if __name__ == "__main__":
    unittest.main()
