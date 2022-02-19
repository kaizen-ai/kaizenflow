import logging
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

import core.config as cconfig
import dataflow.model.experiment_config as dtfmoexcon
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_build_configs_varying_tiled_periods1(hunitest.TestCase):
    def cover_with_monthly_tiles(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        expected_output: str,
        expected_num_configs: int,
    ) -> None:
        # Prepare inputs.
        config = cconfig.Config()
        freq_as_pd_str = "1M"
        # end_timestamp += pd.tseries.frequencies.to_offset("M")
        # dates = pd.date_range(start_timestamp, end_timestamp,
        #                       freq=freq_as_pd_str)
        # print(dates)
        # assert 0
        lookback_as_pd_str = "10D"
        # Run.
        configs = dtfmoexcon.build_configs_varying_tiled_periods(
            config,
            start_timestamp,
            end_timestamp,
            freq_as_pd_str,
            lookback_as_pd_str,
        )
        # Check output.
        actual_output = cconfig.configs_to_str(configs)
        self.assert_equal(actual_output, expected_output, fuzzy_match=True)
        self.assertEqual(len(configs), expected_num_configs)

    def test_1tile_1(self) -> None:
        """
        Cover [2020-01-01, 2020-01-31] with 1 monthly tile.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-01 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-01-31 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""
        # 1/1
        meta:
          start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
          start_timestamp: 2020-01-01 00:00:00+00:00
          end_timestamp: 2020-01-31 00:00:00+00:00"""
        expected_num_configs = 1
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )

    def test_1tile_2(self) -> None:
        """
        Cover [2020-01-01, 2020-01-30] with 1 monthly tile.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-01 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-01-30 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""
        # 1/1
        meta:
          start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
          start_timestamp: 2020-01-01 00:00:00+00:00
          end_timestamp: 2020-01-31 00:00:00+00:00"""
        expected_num_configs = 1
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )

    def test_1tile_3(self) -> None:
        """
        Cover [2020-01-02, 2020-01-30] with 1 monthly tile.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-02 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-01-30 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""
        # 1/1
        meta:
          start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
          start_timestamp: 2020-01-01 00:00:00+00:00
          end_timestamp: 2020-01-31 00:00:00+00:00"""
        expected_num_configs = 1
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )

    # //////////////////////////////////////////////////////////////////////////////

    def test_2tiles_1(self) -> None:
        """
        Cover [2020-01-02, 2020-02-01] with 2 monthly tiles.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-02 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-02-01 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""# 1/2
        meta:
        start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
        start_timestamp: 2020-01-01 00:00:00+00:00
        end_timestamp: 2020-01-31 00:00:00+00:00
        # 2/2
        meta:
        start_timestamp_with_lookback: 2020-01-22 00:00:00+00:00
        start_timestamp: 2020-02-01 00:00:00+00:00
        end_timestamp: 2020-02-29 00:00:00+00:00"""
        expected_num_configs = 2
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )

    def test_2tiles_2(self) -> None:
        """
        Cover [2020-01-07, 2020-02-29] with 2 monthly tiles.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-07 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-02-29 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""
        # 1/2
        meta:
        start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
        start_timestamp: 2020-01-01 00:00:00+00:00
        end_timestamp: 2020-01-31 00:00:00+00:00
        # 2/2
        meta:
        start_timestamp_with_lookback: 2020-01-22 00:00:00+00:00
        start_timestamp: 2020-02-01 00:00:00+00:00
        end_timestamp: 2020-02-29 00:00:00+00:00"""
        expected_num_configs = 2
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )

    # //////////////////////////////////////////////////////////////////////////////

    def test_3tiles_1(self) -> None:
        """
        Cover [2020-01-07, 2020-03-15] with 3 monthly tiles.
        """
        # Inputs.
        start_timestamp = pd.Timestamp("2020-01-07 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2020-03-31 00:00:00+0000", tz="UTC")
        # Expected output.
        expected_output = r"""
        # 1/3
        meta:
          start_timestamp_with_lookback: 2019-12-22 00:00:00+00:00
          start_timestamp: 2020-01-01 00:00:00+00:00
          end_timestamp: 2020-01-31 00:00:00+00:00
        # 2/3
        meta:
          start_timestamp_with_lookback: 2020-01-22 00:00:00+00:00
          start_timestamp: 2020-02-01 00:00:00+00:00
          end_timestamp: 2020-02-29 00:00:00+00:00
        # 3/3
        meta:
          start_timestamp_with_lookback: 2020-02-20 00:00:00+00:00
          start_timestamp: 2020-03-01 00:00:00+00:00
          end_timestamp: 2020-03-31 00:00:00+00:00"""
        expected_num_configs = 3
        # Run.
        self.cover_with_monthly_tiles(
            start_timestamp, end_timestamp, expected_output, expected_num_configs
        )
