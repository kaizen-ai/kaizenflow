import logging

import helpers.timer as timer
import helpers.unit_test as hut


class TestTimedScope(hut.TestCase):
    def test_1(self) -> None:
        """
        Test that elapsed time is correctly extracted.
        """
        # Run the function on test calculations.
        with timer.TimedScope(logging.INFO, "Test calculations") as ts:
            _ = 2**10**8
        # Round actual time up to 1 decimal and compare it with expected.
        actual_rounded_time = round(ts.elapsed_time[-1], 1)
        expected_rounded_time = 0.4
        self.assertEqual(actual_rounded_time, expected_rounded_time)
