import os
from typing import List

import helpers.hgit as hgit
import helpers.hsystem as hsystem
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.common.data.client.test.im_client_test_case as icdctictc


def _generate_test_data(
    instance: icdctictc.ImClientTestCase,
    start_date: str,
    end_date: str,
    freq: str,
    assets: str,
    output_type: str,
    partition_mode: str,
) -> str:
    """
    Generate test data in form of partitioned Parquet files.
    """
    test_dir: str = instance.get_scratch_space()
    tiled_bar_data_dir = os.path.join(test_dir, "tiled.bar_data")
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
    cmd.append(f"--partition_mode {partition_mode}")
    cmd.append(f"--dst_dir {tiled_bar_data_dir}")
    cmd.append(f"--output_type {output_type}")
    cmd = " ".join(cmd)
    hsystem.system(cmd)
    return test_dir


# #############################################################################
# TestHistoricalPqByAssetClient1
# #############################################################################


class HistoricalByTileMock(imvcdchpcl.HistoricalPqByTileClient):
    """
    Only here to allow usage of `HistoricalPqByAssetClient`.
    """

    def get_universe(self) -> List[str]:
        """
        See description in the parent class.
        """
        universe = imvccunun.get_vendor_universe()
        return universe  # type: ignore[no-any-return]


class TestHistoricalPqByTileClient1(icdctictc.ImClientTestCase):
    def test_read_data1(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "1467591036"
        output_type = "verbose_close"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self, start_date, end_date, freq, assets, output_type, partition_mode
        )
        # Init client for testing.
        asset_column_name = "asset_id"
        im_client = HistoricalByTileMock(
            asset_column_name, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbol = "binance::BTC_USDT"
        expected_length = 4320
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {"full_symbol": [1467591036]}
        expected_signature = r"""# df=
        df.index in [2021-12-30 00:00:00+00:00, 2022-01-01 23:59:00+00:00]
        df.columns=full_symbol,close,year,month
        df.shape=(4320, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   1467591036      0  2021    12
        2021-12-30 00:01:00+00:00   1467591036      1  2021    12
        2021-12-30 00:02:00+00:00   1467591036      2  2021    12
        ...
        2022-01-01 23:57:00+00:00   1467591036   4317  2022     1
        2022-01-01 23:58:00+00:00   1467591036   4318  2022     1
        2022-01-01 23:59:00+00:00   1467591036   4319  2022     1"""
        self._test_read_data1(
            im_client,
            full_symbol,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data2(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "1467591036,1508924190"
        output_type = "verbose_close"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self, start_date, end_date, freq, assets, output_type, partition_mode
        )
        # Init client for testing.
        asset_column_name = "asset_id"
        im_client = HistoricalByTileMock(
            asset_column_name, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 8640
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {"full_symbol": [1467591036, 1508924190]}
        expected_signature = r"""# df=
        df.index in [2021-12-30 00:00:00+00:00, 2022-01-01 23:59:00+00:00]
        df.columns=full_symbol,close,year,month
        df.shape=(8640, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   1467591036      0  2021    12
        2021-12-30 00:00:00+00:00   1508924190      0  2021    12
        2021-12-30 00:01:00+00:00   1467591036      1  2021    12
        ...
        2022-01-01 23:58:00+00:00   1508924190   4318  2022     1
        2022-01-01 23:59:00+00:00   1467591036   4319  2022     1
        2022-01-01 23:59:00+00:00   1508924190   4319  2022     1"""
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
