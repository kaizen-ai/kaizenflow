import logging
import os

import pandas as pd

import helpers.unit_test as hut
import helpers.timer as timer


class TestTimedScope(hut.TestCase):
    def _load_test_table(self) -> pd.DataFrame:
        """
        Load a pre-saved Form 4 general info table for testing.

        :return: a pre-saved dataframe
        """
        # Get the input directory path.
        input_dir = self.get_input_dir()
        # Create a full path to the test tables file.
        test_table_path = os.path.join(input_dir, "test_table.csv")
        # Load the test table.
        test_table = pd.read_csv(test_table_path)
        return test_table

    def test_1(self) -> None:
        """
        Test that elapsed time is correctly extracted.
        """
        # Load the test table.
        test_table = self._load_test_table()
        # Sort table in reverse order.
        with timer.TimedScope(logging.INFO, "Sort table in reverse order") as ts:
            test_table.sort_values(
                by=list(test_table.columns),
                ascending=[False] * len(test_table.columns)
            )
        # Set actual and expected outcomes.
        actual_message = ts.elapsed_time[0]
        actual_time = ts.elapsed_time[-1]
        expected_message = "Sort table in reverse order done (0.006 s)"
        expected_time = 0.006
        # Compare actual and expected outcomes.
        self.assertEqual(actual_message, expected_message)
        self.assertEqual(actual_time, expected_time)
