import logging

import numpy as np

import helpers.hnumpy as hhnumpy
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


class TestRandomSeedContext(huntes.TestCase):
    def test_example1(self) -> None:
        """
        Getting more random numbers without context manager changes the
        sequence of random numbers.
        """
        n = 3
        # First batch.
        np.random.seed(0)
        vals1a = np.random.randn(n)
        vals2a = np.random.randn(n)
        # Second batch.
        np.random.seed(0)
        vals1b = np.random.randn(n)
        vals = np.random.randn(n)
        _ = vals
        vals2b = np.random.randn(n)
        # Check.
        self.assertEqual(str(vals1a), str(vals1b))
        # Of course this might fail with a vanishingly small probability.
        self.assertNotEqual(str(vals2a), str(vals2b))

    def test_example2(self) -> None:
        """
        Getting more random numbers with context manager doesn't change the
        sequence of random numbers.
        """
        n = 3
        # First batch.
        np.random.seed(0)
        vals1a = np.random.randn(n)
        vals2a = np.random.randn(n)
        # Second batch.
        np.random.seed(0)
        vals1b = np.random.randn(n)
        with hhnumpy.random_seed_context(42):
            vals = np.random.randn(n)
            _ = vals
        vals2b = np.random.randn(n)
        # Check.
        self.assertEqual(str(vals1a), str(vals1b))
        self.assertEqual(str(vals2a), str(vals2b))
