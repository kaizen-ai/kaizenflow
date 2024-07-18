import logging

import pandas as pd

import helpers.hunit_test as hunitest
import research_amp.soccer_prediction.utils as rasoprut

_LOG = logging.getLogger(__name__)


class TestCalculateRPS(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test RPS for custom dataframe.
        """
        # Test data.
        data = {
            "prob_home_win": [0.7, 0.2, 0.5],
            "prob_draw": [0.2, 0.5, 0.3],
            "prob_away_win": [0.1, 0.3, 0.2],
            "actual_outcome": ["home_win", "draw", "away_win"],
        }
        df_test = pd.DataFrame(data)
        # Expected RPS calculation.
        expected_rps_val = (
            (0.7 - 1) ** 2
            + (0.9 - 1) ** 2
            + (0.2 - 0) ** 2
            + (0.7 - 1) ** 2
            + (0.5 - 0) ** 2
            + (0.8 - 0) ** 2
        ) / 6
        expected_rps = f"{expected_rps_val: .2f}"
        actual_rps_val = rasoprut.calculate_rps(df_test)
        actual_rps = f"{actual_rps_val: .2f}"
        # Check the value.
        self.assert_equal(actual_rps, expected_rps)

    def test2(self) -> None:
        """
        Check the function for custom column names.
        """
        # Test data with custom column names
        data = {
            "home_prob": [0.6, 0.3, 0.4],
            "draw_prob": [0.3, 0.4, 0.4],
            "away_prob": [0.1, 0.3, 0.2],
            "outcome": ["home_win", "draw", "away_win"],
        }
        df_test = pd.DataFrame(data)
        # Expected RPS calculation.
        expected_rps_val = (
            (0.6 - 1) ** 2
            + (0.9 - 1) ** 2
            + (0.3 - 0) ** 2
            + (0.7 - 1) ** 2
            + (0.4 - 0) ** 2
            + (0.8 - 0) ** 2
        ) / 6
        expected_rps = f"{expected_rps_val: .2f}"
        actual_rps_val = rasoprut.calculate_rps(
            df_test,
            prob_home_win_col="home_prob",
            prob_draw_col="draw_prob",
            prob_away_win_col="away_prob",
            actual_outcome_col="outcome",
        )
        actual_rps = f"{actual_rps_val: .2f}"
        # Check the values.
        self.assert_equal(actual_rps, expected_rps)
