import logging
from typing import List, Optional

import core.config as cconfig
import dataflow.model.backtest_notebook_utils as dtfmbanout
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_build_research_backtest_analyzer_config_dict(hunitest.TestCase):
    def get_config(self, sweep_values: Optional[List[float]]) -> cconfig.Config:
        """
        Get default config for testing.
        """
        config_dict = {
            "asset_id_col": "asset_id",
            "pnl_resampling_frequency": "D",
            "rule": "6T",
            "annotate_forecasts_kwargs": {
                "style": "longitudinal",
                "quantization": 30,
                "liquidate_at_end_of_day": False,
                "initialize_beginning_of_day_trades_to_zero": False,
                "burn_in_bars": 3,
                "compute_extended_stats": True,
                "target_dollar_risk_per_name": 1.0,
                "modulate_using_prediction_magnitude": False,
                "prediction_abs_threshold": 0.3,
            },
        }
        if sweep_values is not None:
            config_dict["sweep_param"] = {
                "keys": (
                    "annotate_forecasts_kwargs",
                    "target_dollar_risk_per_name",
                ),
                "values": sweep_values,
            }
        config = cconfig.Config().from_dict(config_dict)
        return config

    def test_sweep_param_one_value(self) -> None:
        """
        Test with one value in `sweep_param` field.
        """
        config_dict = self.get_config(sweep_values=[0.7])
        actual = dtfmbanout.build_research_backtest_analyzer_config_sweep(
            config_dict
        )
        self.check_string(str(actual))

    def test_sweep_param_multiple_values(self) -> None:
        """
        Test with multiple values in `sweep_param` field.
        """
        config = self.get_config(sweep_values=[0.25, 0.5, 1])
        actual = dtfmbanout.build_research_backtest_analyzer_config_sweep(config)
        self.check_string(str(actual))

    def test_no_sweep_param(self) -> None:
        """
        Test without `sweep_param` field.
        """
        config = self.get_config(sweep_values=None)
        actual = dtfmbanout.build_research_backtest_analyzer_config_sweep(config)
        self.check_string(str(actual))
