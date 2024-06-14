import unittest

from utils.Counter import Counter


class TestCounter(unittest.TestCase):
    def test_incr_simple(self):
        Counter().reset()
        x1 = Counter().id
        self.assertEqual(x1, 1)

    def test_incr_multiple(self):
        x2 = 0
        Counter().reset()
        for _ in range(10):
            x2 = Counter().id
        self.assertEqual(x2, 10)

    def test_set(self):
        Counter().reset()
        x3 = Counter().set(100)
        self.assertEqual(x3, 100)

        x3 = Counter().id
        self.assertEqual(x3, 101)
