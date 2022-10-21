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
    _bid_ask_dataframe_sample = None

    @staticmethod
    def get_s3_path() -> str:
        aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
        s3_path = os.path.join(s3_bucket_path, "reorg/daily_staged.airflow.pq")
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
        """
        Get OHLCV data sample.
        """
        ohlcv_sample = pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "full_symbol",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1631145600000, 30, 40, 50, 60, 70, "binance::BTC_USDT"],
                [1631145660000, 31, 41, 51, 61, 71, "binance::BTC_USDT"],
                [1631145840000, 34, 44, 54, 64, 74, "binance::BTC_USDT"],
                [1631145840000, 34, 44, 54, 64, 74, "binance::ETH_USDT"],
                [1631145840000, 34, 44, 54, 64, 74, "kucoin::ETH_USDT"],
                [1631145900000, 38, 39, 50, 61, 71, "binance::SOL_USDT"],
                [1631145940000, 31, 45, 54, 60, 75, "binance::ETH_USDT"],
                [1631145960000, 33, 43, 57, 63, 73, "binance::ADA_USDT"],
                [1631145970000, 34, 40, 52, 62, 72, "binance::SOL_USDT"],
                [1631145990000, 37, 44, 51, 64, 72, "kucoin::ETH_USDT"],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        ohlcv_sample = ohlcv_sample.set_index(["timestamp", "full_symbol"])
        self._ohlcv_dataframe_sample = ohlcv_sample
        return self._ohlcv_dataframe_sample.copy()

    def bid_ask_dataframe_sample(self) -> pd.DataFrame:
        """
        Get bid-ask data sample.
        """
        bid_ask_sample = pd.DataFrame(
            columns=[
                "timestamp",
                "full_symbol",
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1631145600000, "binance::BTC_USDT", 19505.817, 1185.347, 19504.541, 859.472],
                [1631145600000, "binance::ETH_USDT", 1321.790, 7004.761, 1321.699, 3670.681],
                [1631145840000, "binance::BTC_USDT", 19500.714, 1019.649, 19502.706, 1101.954],
                [1631145840000, "binance::ETH_USDT", 1321.400, 3675.864, 1321.278, 4320.231],
                [1631145900000, "binance::BTC_USDT", 19483.345, 1689.938, 19484.335, 1095.325],
                [1631145900000, "binance::ETH_USDT", 1320.022, 4612.925, 1320.102, 6404.481],
                [1631145940000, "binance::BTC_USDT", 19483.128, 1528.736, 19482.422, 958.668],
                [1631145940000, "binance::ETH_USDT", 1319.769, 3754.403, 1319.654, 4775.201],
                [1631145970000, "binance::BTC_USDT", 19473.500, 4.351, 19473.600, 15.578],
                [1631145970000, "binance::ETH_USDT", 1319.030, 43.404, 1319.040, 48.725],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        bid_ask_sample = bid_ask_sample.set_index(["timestamp", "full_symbol"])
        self._bid_ask_dataframe_sample = bid_ask_sample
        return self._bid_ask_dataframe_sample.copy()

    @pytest.mark.slow
    def test_compare_ohlcv_1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        No data missing.
        """
        sample = self.ohlcv_dataframe_sample()
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample
        self._test_function_call("ohlcv", "spot", True)
        #
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)

    @pytest.mark.slow
    def test_compare_ohlcv_2(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing realtime data.
        """
        sample_rt = self.ohlcv_dataframe_sample()
        sample_daily = self.ohlcv_dataframe_sample()
        for position in [1, 3]:
            # Edit original realtime data for mismatch.
            sample_rt.iloc[position, sample_rt.columns.get_loc("open")] = 666
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("ohlcv", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Differing table contents:
        df.shape=(2, 4)
        df.full=
        open            timestamp        full_symbol
        self other
        1   31   666  1631145660000  binance::BTC_USDT
        3   34   666  1631145840000  binance::ETH_USDT
        ################################################################################
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_compare_ohlcv_3(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing daily data.s
        """
        sample_rt = self.ohlcv_dataframe_sample()
        sample_daily = self.ohlcv_dataframe_sample()
        for position in [2, 4]:
            # Edit original realtime data for mismatch.
            sample_daily.iloc[position, sample_daily.columns.get_loc("open")] = 999
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("ohlcv", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Differing table contents:
        df.shape=(2, 4)
        df.full=
        open            timestamp        full_symbol
        self other
        2  999    34  1631145840000  binance::BTC_USDT
        4  999    34  1631145840000   kucoin::ETH_USDT
        ################################################################################
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


    @pytest.mark.slow
    def test_compare_ohlcv_4(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Mismatch between realtime and daily data with missing data.
        """
        sample_rt = self.ohlcv_dataframe_sample().head(4)
        sample_daily = self.ohlcv_dataframe_sample().tail(5)
        for position in [1, 3]:
            # Edit original realtime data for mismatch.
            sample_rt.iloc[position, sample_rt.columns.get_loc("open")] = 666
        for position in [2, 4]:
            # Edit original daily data for mismatch.
            sample_daily.iloc[position, sample_daily.columns.get_loc("open")] = 999
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("ohlcv", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""

        ################################################################################
        Missing real time data:
        df.shape=(5, 5)
        df.full=
                                        open  high  low  close  volume
        timestamp     full_symbol
        1631145900000 binance::SOL_USDT    38    39   50     61      71
        1631145940000 binance::ETH_USDT    31    45   54     60      75
        1631145960000 binance::ADA_USDT   999    43   57     63      73
        1631145970000 binance::SOL_USDT    34    40   52     62      72
        1631145990000 kucoin::ETH_USDT    999    44   51     64      72
        Missing daily data:
        df.shape=(4, 5)
        df.full=
                                        open  high  low  close  volume
        timestamp     full_symbol
        1631145600000 binance::BTC_USDT    30    40   50     60      70
        1631145660000 binance::BTC_USDT   666    41   51     61      71
        1631145840000 binance::BTC_USDT    34    44   54     64      74
                    binance::ETH_USDT   666    44   54     64      74
        ################################################################################
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_compare_bid_ask_1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        No data missing.
        """
        sample = self.bid_ask_dataframe_sample()
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample
        self._test_function_call("bid_ask", "futures", True)
        #
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)

    @pytest.mark.slow
    def test_compare_bid_ask_2(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing realtime data.
        """
        sample_rt = self.bid_ask_dataframe_sample()
        sample_daily = self.bid_ask_dataframe_sample()
        for position in [1, 3]:
            # Edit original realtime data for mismatch.
            sample_rt.iloc[position, sample_rt.columns.get_loc("bid_size")] = 666.667
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("bid_ask", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Difference between bid sizes in real time and daily data for `binance::ETH_USDT` coin is more that 1%
        ################################################################################
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_compare_bid_ask_3(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing daily data.
        """
        sample_rt = self.bid_ask_dataframe_sample()
        sample_daily = self.bid_ask_dataframe_sample()
        for position in [2, 4]:
            # Edit original realtime data for mismatch.
            sample_daily.iloc[position, sample_daily.columns.get_loc("ask_price")] = 999.876
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("bid_ask", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Difference between ask prices in real time and daily data for `binance::BTC_USDT` coin is more that 1%
        ################################################################################
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


    @pytest.mark.slow
    def test_compare_bid_ask_4(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Mismatch between realtime and daily data with missing data.
        """
        sample_rt = self.bid_ask_dataframe_sample().head(4)
        sample_daily = self.bid_ask_dataframe_sample().head(4)
        for position in [1, 3]:
            # Edit original realtime data for mismatch.
            sample_rt.iloc[position, sample_rt.columns.get_loc("bid_size")] = 2279.667
        for position in [2, 3]:
            # Edit original daily data for mismatch.
            sample_daily.iloc[position, sample_daily.columns.get_loc("ask_price")] = 3224.678
        mock_get_rt_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_rt_data"
        )
        mock_get_rt_data = mock_get_rt_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_rt_data.return_value = sample_rt
        #
        mock_get_daily_data_patch = umock.patch.object(
            imvcdecrah.RealTimeHistoricalReconciler, "_get_daily_data"
        )
        mock_get_daily_data = mock_get_daily_data_patch.start()
        # Prepare and attach sample to mocked function.
        mock_get_daily_data.return_value = sample_daily
        # Run.
        with pytest.raises(AssertionError) as fail:
            self._test_function_call("bid_ask", "spot", True)
        # Stop the patches.
        mock_get_rt_data_patch.stop()
        mock_get_daily_data_patch.stop()
        self.assertEqual(mock_get_rt_data.call_count, 1)
        self.assertEqual(mock_get_daily_data.call_count, 1)
        # Check output.
        actual = str(fail.value)
        expected = r"""
        ################################################################################
        Difference between ask prices in real time and daily data for `binance::BTC_USDT` coin is more that 1%
        Difference between ask prices in real time and daily data for `binance::ETH_USDT` coin is more that 1%
        Difference between bid sizes in real time and daily data for `binance::ETH_USDT` coin is more that 1%
        ################################################################################
        """
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
        cmd.extend(["--data_type", "ohlcv"])
        cmd.extend(["--contract_type", "spot"])
        cmd.extend(["--s3_vendor", "crypto_chassis"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        # Change bool values to string type to pass the check.
        actual["resample_1min"] = "True"
        actual["resample_1sec"] = "False"
        expected = {
            "start_timestamp": "20220216-000000",
            "end_timestamp": "20220217-000000",
            "db_stage": "dev",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/reorg/historical.manual.pq/",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "resample_1min": "True",
            "resample_1sec": "False",
            "s3_vendor": "crypto_chassis",
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

    def _test_function_call(self, data_type, contract_type, resample_1min) -> None:
        """
        Test directly _run function for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "20210915-235500",
            "end_timestamp": "20220917-000500",
            "db_stage": "local",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": f"{self.get_s3_path()}",
            "connection": self.connection,
            "data_type": data_type,
            "contract_type": contract_type,
            "resample_1min": True,
            "s3_vendor": "crypto_chassis",
        }
        if resample_1min:
            kwargs["resample_1min"] = True
            kwargs["resample_1sec"] = False
        else:
            kwargs["resample_1min"] = False
            kwargs["resample_1sec"] = True
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdecrah._run(args)


class TestFilterDuplicates(hunitest.TestCase):
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
                "full_symbol",
                "end_download_timestamp",
                "knowledge_timestamp",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [0, 1631145600000, 30, 40, 50, 60, 70, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [1, 1631145660000, 31, 41, 51, 61, 71, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [2, 1631145840000, 34, 44, 54, 64, 74, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [3, 1631145840000, 34, 44, 54, 64, 74, "binance::ETH_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [4, 1631145840000, 34, 44, 54, 64, 74, "kucoin::ETH_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
            ]
            # pylint: enable=line-too-long
            # fmt: on
        )
        return test_data

    def test_filter_duplicates(self) -> None:
        """
        Verify that duplicated data is filtered correctly.
        """
        input_data = self._get_duplicated_test_data()
        self.assertEqual(input_data.shape, (10, 10))
        # Filter duplicates.
        actual_data = imvcdecrah.RealTimeHistoricalReconciler._filter_duplicates(input_data)
        expected_length = 6
        expected_column_names = [
            "close",
            "full_symbol",
            "end_download_timestamp",
            "high",
            "id",
            "knowledge_timestamp",
            "low",
            "open",
            "timestamp",
            "volume",
        ]
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[0, 5]
        columns=id,timestamp,open,high,low,close,volume,full_symbol,end_download_timestamp,knowledge_timestamp
        shape=(6, 10)
        id                 timestamp  open  high  low  close  volume        full_symbol    end_download_timestamp       knowledge_timestamp
        0   1 2021-09-09 00:00:00+00:00    30    40   50     60      70  binance::BTC_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        1   2 2021-09-09 00:01:00+00:00    31    41   51     61      71  binance::BTC_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        2   3 2021-09-09 00:02:00+00:00    32    42   52     62      72  binance::ETH_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        3   4 2021-09-09 00:04:00+00:00    34    44   54     64      74  binance::BTC_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        4   5 2021-09-09 00:04:00+00:00    34    44   54     64      74  binance::ETH_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
        5   6 2021-09-09 00:04:00+00:00    34    44   54     64      74   kucoin::ETH_USDT 2021-09-09 00:00:00+00:00 2021-09-09 00:00:00+00:00
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
                "full_symbol",
                "end_download_timestamp",
                "knowledge_timestamp",
            ],
            # fmt: off
            # pylint: disable=line-too-long
            data=[
                [1, 1631145600000, 30, 40, 50, 60, 70, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [2, 1631145660000, 31, 41, 51, 61, 71, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [3, 1631145720000, 32, 42, 52, 62, 72, "binance::ETH_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [4, 1631145840000, 34, 44, 54, 64, 74, "binance::BTC_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [5, 1631145840000, 34, 44, 54, 64, 74, "binance::ETH_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
                [6, 1631145840000, 34, 44, 54, 64, 74, "kucoin::ETH_USDT", pd.Timestamp("2021-09-09"), pd.Timestamp("2021-09-09")],
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
