import logging
import unittest

from src.utils.Counter import Counter
from src.utils.setup_logger import log


class TestCounter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        new_handlers = []
        for h in log.handlers:
            if not isinstance(h, logging.StreamHandler):
                new_handlers.append(h)
            h.close()
        log.handlers = new_handlers
        log.info("Set up class for testing Counter class.")

    def test_constructor(self):
        counter = Counter()
        self.assertEqual(counter.resource_id, 0)

    def test_incr_simple(self):
        counter = Counter()
        counter.increment()
        self.assertEqual(counter.resource_id, 1)

    def test_incr_multiple(self):
        counter = Counter()
        for _ in range(10):
            counter.increment()
        self.assertEqual(counter.resource_id, 10)

    def test_set(self):
        counter = Counter()
        counter.set(new_value=100)
        self.assertEqual(counter.resource_id, 100)

        counter.increment()
        self.assertEqual(counter.resource_id, 101)

    def test_reset(self):
        counter = Counter()
        counter.increment()
        counter.increment()
        self.assertEqual(counter.resource_id, 2)
        counter.reset()
        self.assertEqual(counter.resource_id, 0)