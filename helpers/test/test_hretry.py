import logging

import helpers.hretry as hretry
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

EXCEPTIONS = (AttributeError, ValueError)

class Test_retry(hunitest.TestCase):
    def test_retry1(self) -> None:
        """
        Test normal case.
        """
        self.exception_count = 0
        num_attempts = 3
        @hretry.retry(num_attempts, EXCEPTIONS)
        def func() -> bool:
            if self.exception_count < num_attempts - 1:
                self.exception_count += 1
                raise ValueError("Simulated expected error")
            _LOG.debug("All good")
            return True
        self.assertTrue(func())
        self.assertEqual(self.exception_count, num_attempts - 1)

    def test_retry2(self) -> None:
        """
        Test when the number of exceptions is greater than the
        number of retries.
        """
        self.exception_count = 0
        num_attempts = 3
        @hretry.retry(num_attempts, EXCEPTIONS)
        def func() -> bool:
            if self.exception_count < num_attempts:
                self.exception_count += 1
                raise ValueError("Simulated expected error")
            _LOG.debug("All good")
            return True 
        with self.assertRaises(ValueError):
            func()      

    def test_retry3(self) -> None:
        """
        Test when the raised exception is not in the
        list of expected exceptions.
        """
        self.exception_count = 0
        num_attempts = 3
        @hretry.retry(num_attempts, EXCEPTIONS)
        def func() -> None:
            if self.exception_count < num_attempts - 1:
                self.exception_count += 1
                raise IndexError("Simulated non expected error")
            _LOG.debug("All good")
        with self.assertRaises(IndexError):
            func()
