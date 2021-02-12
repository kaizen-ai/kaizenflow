import collections
import logging

import pandas as pd
import numpy as np

import core.stats_computer as cstats
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestStatsComputer(hut.TestCase):
    def test_series_stats(self) -> None:
        srs = self._get_srs()["returns"]
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(srs)
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)
        
    def test_model_stats(self) -> None:
        srs = self._get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(srs["pnl"], srs["positions"], srs["returns"])
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)
        
    def test_none_returns(self) -> None:
        srs = self._get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(srs["pnl"], srs["positions"])
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)
        
    def test_only_positions(self) -> None:
        srs = self._get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(srs["positions"])
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)
    
    def _get_srs(self) -> pd.DataFrame:
        df = pd.DataFrame()
        np.random.seed(0)
        for col in ["pnl", "positions", "returns"]:
            df[col] = pd.Series(np.random.normal(size=50))
        df.index=pd.date_range(start="2015-01-01", periods=50, freq="D")
        return df