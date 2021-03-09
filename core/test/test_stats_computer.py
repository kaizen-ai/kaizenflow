import logging

import numpy as np
import pandas as pd

import core.stats_computer as cstats
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestStatsComputer(hut.TestCase):
    def test_all_stats(self) -> None:
        srs = _get_srs()["returns"]
        stats_comp = cstats.StatsComputer()
        stats = stats_comp.summarize_time_index_info(srs)
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)


class TestSeriesStatsComputer(hut.TestCase):
    def test_all_stats(self) -> None:
        srs = _get_srs()["returns"]
        stats_comp = cstats.SeriesStatsComputer()
        stats = stats_comp.calculate_stats(srs)
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)


class TestModelStatsComputer(hut.TestCase):
    def test_all_stats(self) -> None:
        srs = _get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(
            srs["pnl"], srs["positions"], srs["returns"]
        )
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)

    def test_none_returns(self) -> None:
        srs = _get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(srs["pnl"], srs["positions"])
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)

    def test_only_positions(self) -> None:
        srs = _get_srs()
        stats_comp = cstats.ModelStatsComputer()
        stats = stats_comp.calculate_stats(positions=srs["positions"])
        str_output = hut.convert_df_to_string(stats, index=True)
        self.check_string(str_output)


def _get_srs() -> pd.DataFrame:
    df = pd.DataFrame()
    np.random.seed(0)
    for col in ["pnl", "positions", "returns"]:
        df[col] = pd.Series(np.random.normal(size=50))
    df.index = pd.date_range(start="2015-01-01", periods=50, freq="D")
    return df
