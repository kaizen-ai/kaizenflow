import os

import core.information_bars.bars as bars
import helpers.unit_test as hut


class TestBars(hut.TestCase):
    def test_get_tick_bars(self) -> None:
        """Test that tick bar creation works."""
        file_path = self._get_input_file_path()
        actual = bars.get_tick_bars(file_path, threshold=10)
        self.check_string(actual.to_csv())

    def test_get_volume_bars(self) -> None:
        """Test that volume bar creation works."""
        file_path = self._get_input_file_path()
        actual = bars.get_volume_bars(file_path, threshold=10)
        self.check_string(actual.to_csv())

    def test_get_dollar_bars(self) -> None:
        """Test that dollar bar creation works."""
        file_path = self._get_input_file_path()
        actual = bars.get_dollar_bars(file_path, threshold=10)
        self.check_string(actual.to_csv())

    def _get_input_file_path(self) -> str:
        """Get file path to input CSV file for a current test.

        :returns path to input file for this test
        """
        file_name = "input.csv"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        return file_name
