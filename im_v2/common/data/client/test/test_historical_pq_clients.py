import os
from typing import List

import pandas as pd
import pytest

import helpers.hgit as hgit
import helpers.hsystem as hsystem
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.common.data.client.test.im_client_test_case as icdctictc


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


# #############################################################################
# TestHistoricalPqByTileClient1
# #############################################################################


class MockHistoricalByTile(imvcdchpcl.HistoricalPqByTileClient):

    def get_universe(self) -> List[str]:
        return ["binance::BTC_USDT", "kucoin::FIL_USDT"]


class TestHistoricalPqByTileClient1(icdctictc.ImClientTestCase):
    # TODO(gp): @Nikola factor out the common code in the tests so that
    #  it's clear what exactly each method is testing, e.g.,
    #  generate_test_data1(...), get_mock_client1(), ...
    def test_read_data1(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "binance::BTC_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbol = "binance::BTC_USDT"
        expected_length = 4320
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {"full_symbol": ["binance::BTC_USDT"]}
        expected_signature = r"""# df=
        index=[2021-12-30 00:00:00+00:00, 2022-01-01 23:59:00+00:00]
        columns=full_symbol,close,year,month
        shape=(4320, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   binance::BTC_USDT      0  2021    12
        2021-12-30 00:01:00+00:00   binance::BTC_USDT      1  2021    12
        2021-12-30 00:02:00+00:00   binance::BTC_USDT      2  2021    12
        ...
        2022-01-01 23:57:00+00:00   binance::BTC_USDT   4317  2022     1
        2022-01-01 23:58:00+00:00   binance::BTC_USDT   4318  2022     1
        2022-01-01 23:59:00+00:00   binance::BTC_USDT   4319  2022     1"""
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
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 8640
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        }
        expected_signature = r"""# df=
        index=[2021-12-30 00:00:00+00:00, 2022-01-01 23:59:00+00:00]
        columns=full_symbol,close,year,month
        shape=(8640, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   binance::BTC_USDT      0  2021    12
        2021-12-30 00:00:00+00:00   kucoin::FIL_USDT      0  2021    12
        2021-12-30 00:01:00+00:00   binance::BTC_USDT      1  2021    12
        ...
        2022-01-01 23:58:00+00:00   kucoin::FIL_USDT   4318  2022     1
        2022-01-01 23:59:00+00:00   binance::BTC_USDT   4319  2022     1
        2022-01-01 23:59:00+00:00   kucoin::FIL_USDT   4319  2022     1"""
        self._test_read_data2(
            im_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data3(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 2640
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-01-01 02:00:00+00:00, 2022-01-01 23:59:00+00:00]
        columns=full_symbol,close,year,month
        shape=(2640, 4)
                                   full_symbol  close  year month
        timestamp
        2022-01-01 02:00:00+00:00   binance::BTC_USDT   3000  2022     1
        2022-01-01 02:00:00+00:00   kucoin::FIL_USDT   3000  2022     1
        2022-01-01 02:01:00+00:00   binance::BTC_USDT   3001  2022     1
        ...
        2022-01-01 23:58:00+00:00   kucoin::FIL_USDT   4318  2022     1
        2022-01-01 23:59:00+00:00   binance::BTC_USDT   4319  2022     1
        2022-01-01 23:59:00+00:00   kucoin::FIL_USDT   4319  2022     1"""
        start_timestamp = pd.Timestamp("2022-01-01 02:00:00+00:00")
        self._test_read_data3(
            im_client,
            full_symbols,
            start_timestamp,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data4(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 6002
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        }
        expected_signature = r"""# df=
        index=[2021-12-30 00:00:00+00:00, 2022-01-01 02:00:00+00:00]
        columns=full_symbol,close,year,month
        shape=(6002, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   binance::BTC_USDT      0  2021    12
        2021-12-30 00:00:00+00:00   kucoin::FIL_USDT      0  2021    12
        2021-12-30 00:01:00+00:00   binance::BTC_USDT      1  2021    12
        ...
        2022-01-01 01:59:00+00:00   kucoin::FIL_USDT   2999  2022     1
        2022-01-01 02:00:00+00:00   binance::BTC_USDT   3000  2022     1
        2022-01-01 02:00:00+00:00   kucoin::FIL_USDT   3000  2022     1"""
        end_timestamp = pd.Timestamp("2022-01-01 02:00:00+00:00")
        self._test_read_data4(
            im_client,
            full_symbols,
            end_timestamp,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_data5(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 242
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        }
        expected_signature = r"""# df=
        index=[2021-12-31 23:00:00+00:00, 2022-01-01 01:00:00+00:00]
        columns=full_symbol,close,year,month
        shape=(242, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-31 23:00:00+00:00   binance::BTC_USDT   2820  2021    12
        2021-12-31 23:00:00+00:00   kucoin::FIL_USDT   2820  2021    12
        2021-12-31 23:01:00+00:00   binance::BTC_USDT   2821  2021    12
        ...
        2022-01-01 00:59:00+00:00   kucoin::FIL_USDT   2939  2022     1
        2022-01-01 01:00:00+00:00   binance::BTC_USDT   2940  2022     1
        2022-01-01 01:00:00+00:00   kucoin::FIL_USDT   2940  2022     1"""
        start_timestamp = pd.Timestamp("2021-12-31 23:00:00+00:00")
        end_timestamp = pd.Timestamp("2022-01-01 01:00:00+00:00")
        self._test_read_data5(
            im_client,
            full_symbols,
            start_timestamp,
            end_timestamp,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    @pytest.mark.skip("CMTask1510: Faulty symbol not detected.")
    def test_read_data6(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-01"
        freq = "1T"
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        full_symbol = "kucoin::MOCK"
        self._test_read_data6(im_client, full_symbol)

    def test_read_data7(self) -> None:
        # TODO(Nina): will fix it in another PR by 'spoiling' the stored test data
        #  so we can demonstrate that everything works.
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-02"
        freq = "1T"
        assets = "binance::BTC_USDT,kucoin::FIL_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = False
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbols = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_length = 8640
        expected_column_names = ["close", "full_symbol", "month", "year"]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        }
        expected_signature = r"""# df=
        index=[2021-12-30 00:00:00+00:00, 2022-01-01 23:59:00+00:00]
        columns=full_symbol,close,year,month
        shape=(8640, 4)
                                   full_symbol  close  year month
        timestamp
        2021-12-30 00:00:00+00:00   binance::BTC_USDT      0  2021    12
        2021-12-30 00:00:00+00:00   kucoin::FIL_USDT      0  2021    12
        2021-12-30 00:01:00+00:00   binance::BTC_USDT      1  2021    12
        ...
        2022-01-01 23:58:00+00:00   kucoin::FIL_USDT   4318  2022     1
        2022-01-01 23:59:00+00:00   binance::BTC_USDT   4319  2022     1
        2022-01-01 23:59:00+00:00   kucoin::FIL_USDT   4319  2022     1"""
        self._test_read_data7(
            im_client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    # ////////////////////////////////////////////////////////////////////////

    def test_get_start_ts_for_symbol1(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-01"
        freq = "1T"
        assets = "binance::BTC_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbol = "binance::BTC_USDT"
        expected_start_timestamp = pd.Timestamp("2021-12-30 00:00:00+00:00")
        self._test_get_start_ts_for_symbol1(
            im_client, full_symbol, expected_start_timestamp
        )

    def test_get_end_ts_for_symbol1(self) -> None:
        # Generate Parquet test data.
        start_date = "2021-12-30"
        end_date = "2022-01-01"
        freq = "1T"
        assets = "binance::BTC_USDT"
        asset_col_name = "full_symbol"
        output_type = "cm_task_1103"
        partition_mode = "by_year_month"
        test_dir = _generate_test_data(
            self,
            start_date,
            end_date,
            freq,
            assets,
            asset_col_name,
            output_type,
            partition_mode,
        )
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        full_symbol = "binance::BTC_USDT"
        expected_end_timestamp = pd.Timestamp("2021-12-31 23:59:00+00:00")
        self._test_get_end_ts_for_symbol1(
            im_client, full_symbol, expected_end_timestamp
        )

    # ////////////////////////////////////////////////////////////////////////

    def test_get_universe1(self) -> None:
        # Init client for testing.
        resample_1min = True
        vendor = "mock"
        test_dir = "dummy"
        partition_mode = "by_year_month"
        im_client = MockHistoricalByTile(
            vendor, resample_1min, test_dir, partition_mode
        )
        # Compare the expected values.
        expected_length = 2
        expected_first_elements = ["binance::BTC_USDT", "kucoin::FIL_USDT"]
        expected_last_elements = expected_first_elements
        self._test_get_universe1(
            im_client,
            expected_length,
            expected_first_elements,
            expected_last_elements,
        )
