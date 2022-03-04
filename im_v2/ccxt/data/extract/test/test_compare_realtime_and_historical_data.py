import argparse
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hparquet as hparque
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestCompareRealtimeAndHistoricalData1(imvcddbut.TestImDbHelper):
    S3_PATH = "s3://cryptokaizen-data/unit-test/parquet/historical"
    FILTERS = [
        [
            ("year", "==", 2021),
            ("month", ">=", 12),
            ("year", "==", 2021),
            ("month", "<=", 12),
        ],
        [
            ("year", "==", 2022),
            ("month", ">=", 1),
            ("year", "==", 2022),
            ("month", "<=", 1),
        ],
    ]
    _ohlcv_dataframe_sample = None

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
            file_name = f"{self.S3_PATH}/binance/"
            ohlcv_sample = hparque.from_parquet(
                file_name, filters=self.FILTERS, aws_profile="ck"
            )
            # Matching exact timespan as in test function call.
            self._ohlcv_dataframe_sample = ohlcv_sample.loc[
                "2021-12-31 23:55:00":"2022-01-01 00:05:00"
            ]
        # Deep copy (which is default for `pd.DataFrame.copy()`) is used to
        # preserve original data for each test.
        return self._ohlcv_dataframe_sample.copy()

    def test_function_call1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Both realtime and daily data are the same.
        """
        sample = self.ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        self._test_function_call()

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
        df.head=
                                       open    high     low   close   volume
        timestamp     currency_pair
        1640995020000 SOL_USDT       170.17  170.29  170.11  170.19   747.91
        1640995080000 SOL_USDT       170.20  170.20  169.76  169.93  1203.06
        1640995140000 SOL_USDT       169.90  170.07  169.90  169.99   304.42
        1640995200000 SOL_USDT       170.01  170.22  169.93  170.14   810.27
        df.tail=
                                       open    high     low   close   volume
        timestamp     currency_pair
        1640995320000 SOL_USDT       170.46  170.60  170.33  170.53   520.30
        1640995380000 SOL_USDT       170.53  170.60  170.27  170.34  1175.03
        1640995440000 SOL_USDT       170.33  170.49  170.31  170.40   318.96
        1640995500000 SOL_USDT       170.41  170.73  170.25  170.65   612.02
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

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
            (f"{self.S3_PATH}/binance/",),
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
        df.head=
                                      open  high    low  close    volume
        timestamp     currency_pair
        1640994900000 ADA_USDT       1.309  1.31  1.308  1.309   47265.6
        1640994960000 ADA_USDT       1.309  1.31  1.308  1.309   45541.6
        1640995020000 ADA_USDT       1.309  1.31  1.309  1.310   23032.3
        1640995080000 ADA_USDT       1.310  1.31  1.306  1.306  179644.8
        df.tail=
                                      open   high    low  close    volume
        timestamp     currency_pair
        1640995200000 ADA_USDT       1.308  1.310  1.307  1.310   98266.8
        1640995260000 ADA_USDT       1.310  1.314  1.308  1.312  132189.4
        1640995320000 ADA_USDT       1.312  1.318  1.311  1.317  708964.2
        1640995380000 ADA_USDT       1.317  1.317  1.315  1.315  219213.9
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

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
        df.head=
                                       open   high     low   close   volume
        timestamp     currency_pair
        1640995320000 SOL_USDT       170.46  170.6  170.33  170.53   520.30
        1640995380000 SOL_USDT       170.53  170.6  170.27  170.34  1175.03
        df.tail=
                                       open    high     low   close  volume
        timestamp     currency_pair
        1640995440000 SOL_USDT       170.33  170.49  170.31  170.40  318.96
        1640995500000 SOL_USDT       170.41  170.73  170.25  170.65  612.02
        Missing daily data:
        df.shape=(4, 5)
        df.head=
                                      open  high    low  close   volume
        timestamp     currency_pair
        1640994900000 ADA_USDT       1.309  1.31  1.308  1.309  47265.6
        1640994960000 ADA_USDT       1.309  1.31  1.308  1.309  45541.6
        df.tail=
                                      open  high    low  close    volume
        timestamp     currency_pair
        1640995020000 ADA_USDT       1.309  1.31  1.309  1.310   23032.3
        1640995080000 ADA_USDT       1.310  1.31  1.306  1.306  179644.8
        Differing table contents:
        df.shape=(6, 2)
        df.head=
                open
                self    other
        2   46285.79  666.000
        33    109.41  666.000
        46    999.00    3.033
        df.tail=
               open
               self    other
        51  999.000  109.750
        67    1.317  666.000
        83  999.000    1.315
        ################################################################################"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_parser(self) -> None:
        """
        Tests arg parser for predefined args in the script.

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
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/historical/"])
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
            "s3_path": "s3://cryptokaizen-data/historical/",
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
        Tests directly _run function for coverage increase.
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
            "s3_path": f"{self.S3_PATH}/",
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdecrah._run(args)
