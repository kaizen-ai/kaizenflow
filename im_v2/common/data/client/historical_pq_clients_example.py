"""
Import as:

import im_v2.common.data.client.historical_pq_clients_example as ivcdchpqce
"""

import os
import logging
from typing import List

import helpers.hgit as hgit
import helpers.hsystem as hsystem
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.common.data.client.test.im_client_test_case as icdctictc

_LOG = logging.getLogger(__name__)


def _generate_test_data(
    instance: icdctictc.ImClientTestCase,
    start_date: str,
    end_date: str,
    freq: str,
    assets: str,
    asset_col_name: str,
    output_type: str,
    partition_mode: str,
) -> str:
    """
    Generate test data in form of partitioned Parquet files.
    """
    test_dir: str = instance.get_scratch_space()
    tiled_bar_data_dir = os.path.join(test_dir, "tiled.bar_data")
    # TODO(gp): @all replace the script with calling the library directly.
    cmd = []
    file_path = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/common/test/generate_pq_test_data.py",
    )
    cmd.append(file_path)
    cmd.append(f"--start_date {start_date}")
    cmd.append(f"--end_date {end_date}")
    cmd.append(f"--freq {freq}")
    cmd.append(f"--assets {assets}")
    cmd.append(f"--asset_col_name {asset_col_name}")
    cmd.append(f"--partition_mode {partition_mode}")
    cmd.append(f"--dst_dir {tiled_bar_data_dir}")
    cmd.append(f"--output_type {output_type}")
    cmd = " ".join(cmd)
    hsystem.system(cmd)
    return test_dir


class MockHistoricalByTileClient(imvcdchpcl.HistoricalPqByTileClient):

    def get_universe(self) -> List[str]:
        return ["binance::BTC_USDT", "kucoin::FIL_USDT"]


def get_MockHistoricalByTileClient_example1(
    instance: icdctictc.ImClientTestCase,
    assets: str,
    start_date: str,
    end_date: str,
    resample_1min: bool,
) -> imvcdchpcl.HistoricalPqByTileClient:
    """
    Build mock client example for tests.
    """
    freq = "1T"
    asset_col_name = "full_symbol"
    output_type = "cm_task_1103"
    partition_mode = "by_year_month"
    test_dir = _generate_test_data(
        instance,
        start_date,
        end_date,
        freq,
        assets,
        asset_col_name,
        output_type,
        partition_mode,
    )
    # Init client for testing.
    vendor = "mock"
    im_client = MockHistoricalByTileClient(
        vendor, test_dir, resample_1min, partition_mode
    )
    return im_client