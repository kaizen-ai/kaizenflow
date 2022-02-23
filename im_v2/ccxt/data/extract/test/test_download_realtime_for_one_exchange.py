import argparse
import os

import pytest

import unittest.mock as umock
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsql as hsql
import helpers.hpandas as hpandas
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import helpers.hs3 as hs3
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut
import im_v2.ccxt.data.extract.download_realtime_for_one_exchange as imvcdedrfoe


class TestDownloadRealtimeForOneExchange1(imvcddbut.TestImDbHelper):
    def setUp(self) -> None:
        super().setUp()
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)
        # TODO(Nikola): Add delete option for s3fs?
        # fs = hs3.get_s3fs("ck")

    @pytest.mark.slow
    @umock.patch.object(imvcdedrfoe.hdateti, "get_current_time")
    def test_function_call1(self, mock_get_current_time: umock.MagicMock) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and comparing function output with predefined directory
        structure and file contents.
        """
        mock_get_current_time.return_value = "2021-11-10 00:00:00.000000+00:00"
        self._test_function_call()
        # Check number of calls and args for current time.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        # Get saved data in db.
        select_all_query = "SELECT * FROM ccxt_ohlcv;"
        actual_df = hsql.execute_query_to_df(self.connection, select_all_query)
        # Check data output.
        actual = hpandas.df_to_str(actual_df, num_rows=5000)
        # pylint: disable=line-too-long
        expected = r"""        id      timestamp     open     high      low    close    volume currency_pair exchange_id end_download_timestamp knowledge_timestamp
        0        1  1636539060000    2.227    2.228    2.225    2.225  71884.50      ADA_USDT     binance             2021-11-10          2021-11-10
        1        2  1636539120000    2.226    2.228    2.225    2.227  64687.00      ADA_USDT     binance             2021-11-10          2021-11-10
        2        3  1636539180000    2.228    2.232    2.227    2.230  59076.30      ADA_USDT     binance             2021-11-10          2021-11-10
        3        4  1636539240000    2.230    2.233    2.230    2.231  58236.20      ADA_USDT     binance             2021-11-10          2021-11-10
        4        5  1636539300000    2.232    2.232    2.228    2.232  62120.70      ADA_USDT     binance             2021-11-10          2021-11-10
        ...    ...            ...      ...      ...      ...      ...       ...           ...         ...                    ...                 ...
        4495  4496  1636568760000  240.930  241.090  240.850  240.990    507.21      SOL_USDT     binance             2021-11-10          2021-11-10
        4496  4497  1636568820000  240.990  241.010  240.800  241.010    623.65      SOL_USDT     binance             2021-11-10          2021-11-10
        4497  4498  1636568880000  241.010  241.420  241.010  241.300    705.84      SOL_USDT     binance             2021-11-10          2021-11-10
        4498  4499  1636568940000  241.300  241.680  241.240  241.660    864.55      SOL_USDT     binance             2021-11-10          2021-11-10
        4499  4500  1636569000000  241.660  241.670  241.410  241.430    762.90      SOL_USDT     binance             2021-11-10          2021-11-10

        [4500 rows x 11 columns]"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    # TODO(Nikola): Do we want to test command line in this test class?
    # def _test_command_line(self, verbose: bool) -> None:
    #     """
    #     Generate daily data for 3 days in a by-date format and then convert it
    #     to by-asset.
    #     """
    #     test_dir, by_date_dir = self.generate_test_data(verbose)
    #     # Build command line to convert the data.
    #     cmd = []
    #     file_path = os.path.join(
    #         hgit.get_amp_abs_path(),
    #         "im_v2/common/data/transform/transform_pq_by_date_to_by_asset.py",
    #     )
    #     cmd.append(file_path)
    #     cmd.append(f"--src_dir {by_date_dir}")
    #     by_asset_dir = os.path.join(test_dir, "by_asset")
    #     cmd.append(f"--dst_dir {by_asset_dir}")
    #     cmd.append("--num_threads 1")
    #     if verbose:
    #         cmd.append("--asset_col_name ticker")
    #     cmd = " ".join(cmd)
    #     hsystem.system(cmd)
    #     self.check_directory_structure_with_file_contents(
    #         by_date_dir, by_asset_dir
    #     )

    # TODO(Nikola): Add test for parser? At least for coverage increase and to know when contract is changed.
    #   It is a big chunk of coverage.
    # def test_parser(self):
    #     """
    #     Tests arg parser for predefined args in the script.
    #     """
    #     parser = imvcdtcpbdtba._parse()
    #     cmd = []
    #     cmd.extend(["--src_dir", "dummy_by_date_dir"])
    #     cmd.extend(["--dst_dir", "dummy_by_asset_dir"])
    #     cmd.extend(["--num_threads", "1"])
    #     cmd.extend(["--asset_col_name", "ticker"])
    #     args = parser.parse_args(cmd)
    #     actual = vars(args)
    #     expected = {
    #         "src_dir": "dummy_by_date_dir",
    #         "dst_dir": "dummy_by_asset_dir",
    #         "asset_col_name": "ticker",
    #         "num_threads": "1",
    #         "dry_run": False,
    #         "no_incremental": False,
    #         "skip_on_error": False,
    #         "num_attempts": 1,
    #         "log_level": "INFO",
    #     }
    #     self.assertDictEqual(actual, expected)

    def _test_function_call(self) -> None:
        """
        Tests directly _run function for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            'start_timestamp': '20211110-101100',
            'end_timestamp': '20211110-101200',
            'exchange_id': 'binance',
            'universe': 'v03',
            'db_stage': 'local',
            'db_table': 'ccxt_ohlcv',
            'incremental': False,
            'log_level': 'INFO',
            'aws_profile': None,
            's3_path': None,
            # TODO(Nikola): Enable when convention is finalized.
            # 'aws_profile': 'ck',
            # 's3_path': 's3://tests/cm_task_1100/realtime/'
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdedrfoe._run(args)
