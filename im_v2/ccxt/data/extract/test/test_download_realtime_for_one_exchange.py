import argparse
import unittest.mock as umock

import pytest

import helpers.hgit as hgit
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.download_realtime_for_one_exchange as imvcdedrfoe
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.common.data.extract.extract_utils as imvcdeexut


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available")
class TestDownloadRealtimeForOneExchange1(hunitest.TestCase):
  
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 1000
      
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdedrfoe._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "20211110-101100"])
        cmd.extend(["--end_timestamp", "20211110-101200"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--db_stage", "dev"])
        cmd.extend(["--db_table", "ccxt_ohlcv"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/realtime/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "20211110-101100",
            "end_timestamp": "20211110-101200",
            "exchange_id": "binance",
            "universe": "v3",
            "db_stage": "dev",
            "db_table": "ccxt_ohlcv",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/realtime/",
        }
        self.assertDictEqual(actual, expected)

    @umock.patch.object(imvcdeexut, "download_realtime_for_one_exchange")
    def test_main(self, mock_download_realtime: umock.MagicMock) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "start_timestamp": "20211110-101100",
            "end_timestamp": "20211110-101200",
            "exchange_id": "binance",
            "universe": "v3",
            "db_stage": "local",
            "db_table": "ccxt_ohlcv",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://mock_bucket",
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        imvcdedrfoe._main(mock_argument_parser)
        # Check call.
        self.assertEqual(len(mock_download_realtime.call_args), 2)
        self.assertEqual(mock_download_realtime.call_args.args[0], namespace)
        self.assertEqual(
            mock_download_realtime.call_args.args[1], imvcdeexcl.CcxtExchange
        )
        # Get saved data in db.
        select_all_query = "SELECT * FROM ccxt_ohlcv;"
        actual_df = hsql.execute_query_to_df(self.connection, select_all_query)
        # Check data output.
        actual = hpandas.df_to_str(actual_df, num_rows=5000)
        # pylint: disable=line-too-long
        expected = r"""        id      timestamp     open     high      low    close    volume currency_pair exchange_id end_download_timestamp knowledge_timestamp
        0        1  1636539060000    2.227    2.228    2.225    2.225  71884.50      ADA_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        1        2  1636539120000    2.226    2.228    2.225    2.227  64687.00      ADA_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        2        3  1636539180000    2.228    2.232    2.227    2.230  59076.30      ADA_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        3        4  1636539240000    2.230    2.233    2.230    2.231  58236.20      ADA_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        4        5  1636539300000    2.232    2.232    2.228    2.232  62120.70      ADA_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        ...    ...            ...      ...      ...      ...      ...       ...           ...         ...                    ...                 ...
        4495  4496  1636568760000  240.930  241.090  240.850  240.990    507.21      SOL_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        4496  4497  1636568820000  240.990  241.010  240.800  241.010    623.65      SOL_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        4497  4498  1636568880000  241.010  241.420  241.010  241.300    705.84      SOL_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        4498  4499  1636568940000  241.300  241.680  241.240  241.660    864.55      SOL_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
        4499  4500  1636569000000  241.660  241.670  241.410  241.430    762.90      SOL_USDT     binance    2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00

        [4500 rows x 11 columns]"""
        self.assert_equal(actual, expected, fuzzy_match=True)
        
