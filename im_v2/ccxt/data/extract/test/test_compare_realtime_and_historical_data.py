import argparse
import os
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hdatetime as hdateti
import helpers.henv as henv
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestCompareRealtimeAndHistoricalData1(imvcddbut.TestImDbHelper):
    FILTERS = [
        [("year", "==", 2021), ("month", ">=", 12)],
        [("year", "==", 2022), ("month", "<=", 1)],
    ]
    _ohlcv_dataframe_sample = None

    @staticmethod
    def get_s3_path() -> str:
        aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
        s3_path = os.path.join(s3_bucket_path, "unit_test/parquet/historical")
        return s3_path

    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()
        # Initialize database.
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        # Drop table used in tests.
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def ohlcv_dataframe_sample(self) -> pd.DataFrame:
        if self._ohlcv_dataframe_sample is None:
            file_name = f"{self.get_s3_path()}/binance/"
            ohlcv_sample = hparque.from_parquet(
                file_name, filters=self.FILTERS, aws_profile="ck"
            )
            # Matching exact timespan as in test function call.
            self._ohlcv_dataframe_sample = ohlcv_sample.loc[
                "2021-12-31 23:55:00":"2022-01-01 00:05:00"  # type: ignore[misc]
            ]
        # Deep copy (which is default for `pd.DataFrame.copy()`) is used to
        # preserve original data for each test.
        return self._ohlcv_dataframe_sample.copy()

    @pytest.mark.slow
    def test_function_call1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Both realtime and daily data are the same.
        """
        sample = self.ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        self._test_function_call()

    @pytest.mark.slow
    def test_function_call2(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing realtime data.
        """
        # Prepare and save sample data in db.
        sample = self.ohlcv_dataframe_sample().head(90)
        self._save_sample_in_db(sample)
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call()
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Missing real time data:
        df.shape=(9, 5)
        df.full=
                                       open    high     low   close   volume
        timestamp     currency_pair
        1640995020000 SOL_USDT       170.17  170.29  170.11  170.19   747.91
        1640995080000 SOL_USDT       170.20  170.20  169.76  169.93  1203.06
        1640995140000 SOL_USDT       169.90  170.07  169.90  169.99   304.42
        1640995200000 SOL_USDT       170.01  170.22  169.93  170.14   810.27
        1640995260000 SOL_USDT       170.09  170.52  170.06  170.40  1504.74
        1640995320000 SOL_USDT       170.46  170.60  170.33  170.53   520.30
        1640995380000 SOL_USDT       170.53  170.60  170.27  170.34  1175.03
        1640995440000 SOL_USDT       170.33  170.49  170.31  170.40   318.96
        1640995500000 SOL_USDT       170.41  170.73  170.25  170.65   612.02
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_function_call3(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing daily data.
        """
        # Prepare and save sample data in db.
        sample = self.ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        # Mock `from_parquet` so data is obtained through mock instead of s3.
        mock_from_parquet_patch = umock.patch.object(
            imvcdecrah.hparque, "from_parquet"
        )
        mock_from_parquet = mock_from_parquet_patch.start()
        mock_from_parquet.return_value = self.ohlcv_dataframe_sample().tail(90)
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call()
        # Stop `from_parquet` mock and check calls and arguments.
        mock_from_parquet_patch.stop()
        self.assertEqual(mock_from_parquet.call_count, 1)
        self.assertEqual(
            mock_from_parquet.call_args.args,
            (f"{self.get_s3_path()}/binance/",),
        )
        self.assertEqual(
            mock_from_parquet.call_args.kwargs,
            {
                "filters": self.FILTERS,
                "aws_profile": "ck",
            },
        )
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Missing daily data:
        df.shape=(9, 5)
        df.full=
                                      open   high    low  close    volume
        timestamp     currency_pair
        1640994900000 ADA_USDT       1.309  1.310  1.308  1.309   47265.6
        1640994960000 ADA_USDT       1.309  1.310  1.308  1.309   45541.6
        1640995020000 ADA_USDT       1.309  1.310  1.309  1.310   23032.3
        1640995080000 ADA_USDT       1.310  1.310  1.306  1.306  179644.8
        1640995140000 ADA_USDT       1.306  1.310  1.306  1.308  373664.9
        1640995200000 ADA_USDT       1.308  1.310  1.307  1.310   98266.8
        1640995260000 ADA_USDT       1.310  1.314  1.308  1.312  132189.4
        1640995320000 ADA_USDT       1.312  1.318  1.311  1.317  708964.2
        1640995380000 ADA_USDT       1.317  1.317  1.315  1.315  219213.9
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_function_call4(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Mismatch between realtime and daily data with missing data.
        """
        # Prepare and save sample data in db.
        sample = self.ohlcv_dataframe_sample().head(95)
        for position in [8, 15, 33]:
            # Edit original realtime data for mismatch.
            sample.iloc[position, sample.columns.get_loc("open")] = 666
        self._save_sample_in_db(sample)
        # Mock `from_parquet` so data is obtained through mock instead of s3.
        mock_from_parquet_patch = umock.patch.object(
            imvcdecrah.hparque, "from_parquet"
        )
        mock_from_parquet = mock_from_parquet_patch.start()
        # Prepare and attach sample to mocked function.
        mock_parquet_sample = self.ohlcv_dataframe_sample().tail(95)
        for position in [6, 13, 56]:
            # Edit original daily data for mismatch.
            mock_parquet_sample.iloc[
                position, mock_parquet_sample.columns.get_loc("open")
            ] = 999
        mock_from_parquet.return_value = mock_parquet_sample
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call()
        # Args and calls are checked in previous test.
        mock_from_parquet_patch.stop()
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Missing real time data:
        df.shape=(4, 5)
        df.full=
                                       open    high     low   close   volume
        timestamp     currency_pair
        1640995320000 SOL_USDT       170.46  170.60  170.33  170.53   520.30
        1640995380000 SOL_USDT       170.53  170.60  170.27  170.34  1175.03
        1640995440000 SOL_USDT       170.33  170.49  170.31  170.40   318.96
        1640995500000 SOL_USDT       170.41  170.73  170.25  170.65   612.02
        Missing daily data:
        df.shape=(4, 5)
        df.full=
                                      open  high    low  close    volume
        timestamp     currency_pair
        1640994900000 ADA_USDT       1.309  1.31  1.308  1.309   47265.6
        1640994960000 ADA_USDT       1.309  1.31  1.308  1.309   45541.6
        1640995020000 ADA_USDT       1.309  1.31  1.309  1.310   23032.3
        1640995080000 ADA_USDT       1.310  1.31  1.306  1.306  179644.8
        Differing table contents:
        df.shape=(6, 4)
        df.full=
                open              timestamp currency_pair
                self   other
        2   46285.79   666.0  1640994900000      BTC_USDT
        33    109.41   666.0  1640995140000     AVAX_USDT
        46     999.0   3.033  1640995200000      EOS_USDT
        51     999.0  109.75  1640995260000     AVAX_USDT
        67     1.317   666.0  1640995380000      ADA_USDT
        83     999.0   1.315  1640995500000      ADA_USDT
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdecrah._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "20220216-000000"])
        cmd.extend(["--end_timestamp", "20220217-000000"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--db_stage", "dev"])
        cmd.extend(["--db_table", "ccxt_ohlcv"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(
            ["--s3_path", "s3://cryptokaizen-data/reorg/historical.manual.pq/"]
        )
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "20220216-000000",
            "end_timestamp": "20220217-000000",
            "db_stage": "dev",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/reorg/historical.manual.pq/",
        }
        self.assertDictEqual(actual, expected)

    def _save_sample_in_db(self, sample: pd.DataFrame) -> None:
        """
        Save small sample of ohlcv data in test db.

        Extra date columns `year` and `month` are removed from sample as
        those are only used for data partition.
        """
        columns = ["year", "month"]
        trimmed_sample = sample.drop(columns, axis=1)
        hsql.execute_insert_query(
            connection=self.connection,
            obj=trimmed_sample,
            table_name="ccxt_ohlcv",
        )

    def _test_function_call(self) -> None:
        """
        Test directly _run function for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "20211231-235500",
            "end_timestamp": "20220101-000500",
            "db_stage": "local",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": f"{self.get_s3_path()}/",
            "connection": self.connection,
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdecrah._run(args)


class TestFilterDuplicates(hunitest.TestCase):
    def test_filter_duplicates(self) -> None:
        """
        Verify that duplicated data is filtered correctly.
        """
        input_data = self._get_duplicated_test_data()
        self.assertEqual(input_data.shape, (10, 11))
        # Filter duplicates.
        actual_data = imvcdecrah._filter_duplicates(input_data)
        expected_length = 6
        expected_column_names = [
            "close",
            "currency_pair",
            "end_download_timestamp",
            "exchange_id",
            "high",
            "id",
            "knowledge_timestamp",
            "low",
            "open",
            "timestamp",
            "volume",
        ]
        # pylint: disable=line-too-long
        expected_signature = """
        # df=
        index=[0, 5]
        columns=id,timestamp,open,high,low,close,volume,currency_pair,exchange_id,end_download_timestamp,knowledge_timestamp
        shape=(6, 11)
        id                 timestamp  open  high  low  close  volume currency_pair exchange_id    end_download_timestamp       knowledge_timestamp
        0   1 2021-09-09 00:00:00+00:00    30    40   50     60      70      BTC_USDT     binance 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        1   2 2021-09-09 00:01:00+00:00    31    41   51     61      71      BTC_USDT     binance 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        2   3 2021-09-09 00:02:00+00:00    32    42   52     62      72      ETH_USDT     binance 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        3   4 2021-09-09 00:04:00+00:00    34    44   54     64      74      BTC_USDT     binance 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        4   5 2021-09-09 00:04:00+00:00    34    44   54     64      74      ETH_USDT     binance 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        5   6 2021-09-09 00:04:00+00:00    34    44   54     64      74      ETH_USDT      kucoin 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        """
        # pylint: enable=line-too-long
        self.check_df_output(
            actual_data,
            expected_length,
            expected_column_names,
            None,
            expected_signature,
        )

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        """
        Create a test CCXT OHLCV dataframe.
        """
        test_data = pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "currency_pair",
                "exchange_id",
                "end_download_timestamp",
                "knowledge_timestamp",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "ETH_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [4, 1631145840000, 34, 44, 54, 64, 74, "BTC_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "ETH_USDT", "binance", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [6, 1631145840000, 34, 44, 54, 64, 74, "ETH_USDT", "kucoin", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        return test_data

    def _get_duplicated_test_data(self) -> pd.DataFrame:
        """
        Get test data with duplicates for `_filter_duplicates` method test.
        """
        test_data = self._get_test_data()
        #
        # TODO(Danya): Add timezone info to test data in client tests.
        test_data["knowledge_timestamp"] = test_data[
            "knowledge_timestamp"
        ].dt.tz_localize("UTC")
        test_data["end_download_timestamp"] = test_data[
            "end_download_timestamp"
        ].dt.tz_localize("UTC")
        test_data["timestamp"] = test_data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        # Add duplicated rows.
        dupes = test_data.loc[0:3]
        dupes["knowledge_timestamp"] = dupes[
            "knowledge_timestamp"
        ] - pd.Timedelta("40s")
        dupes["end_download_timestamp"] = dupes[
            "end_download_timestamp"
        ] - pd.Timedelta("40s")
        test_data = pd.concat([test_data, dupes]).reset_index(drop=True)
        return test_data
