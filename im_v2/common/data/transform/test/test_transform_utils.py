import pandas as pd
import pytest

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.transform_utils as imvcdttrut


class TestConvertTimestampColumn(hunitest.TestCase):
    def test_integer_datetime(self) -> None:
        """
        Verify that integer datetime is converted correctly.
        """
        # Prepare inputs.
        test_data = pd.Series([1638756800000, 1639656800000, 1648656800000])
        # Run.
        actual = imvcdttrut.convert_timestamp_column(test_data)
        # Check output.
        actual = str(actual)
        expected = """
        0   2021-12-06 02:13:20+00:00
        1   2021-12-16 12:13:20+00:00
        2   2022-03-30 16:13:20+00:00
        dtype: datetime64[ns, UTC]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_string_datetime(self) -> None:
        """
        Verify that string datetime is converted correctly.
        """
        # Prepare inputs.
        test_data = pd.Series(["2021-01-12", "2021-02-14", "2010-12-11"])
        # Run.
        actual = imvcdttrut.convert_timestamp_column(test_data)
        # Check output.
        actual = str(actual)
        expected = """
        0   2021-01-12
        1   2021-02-14
        2   2010-12-11
        dtype: datetime64[ns]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_incorrect_datetime(self) -> None:
        """
        Assert that incorrect types are not converted.
        """
        test_data = pd.Series([37.9, 88.11, 14.0])
        with self.assertRaises(ValueError) as fail:
            imvcdttrut.convert_timestamp_column(test_data)
        actual = str(fail.exception)
        expected = (
            "Incorrect data format. Datetime column should be of int or str dtype"
        )
        self.assert_equal(actual, expected)


# #############################################################################


class TestReindexOnDatetime(hunitest.TestCase):
    def get_dummy_df_with_timestamp(self, unit: str = "ms") -> pd.DataFrame:
        datetime_column_name = "dummy_timestamp"
        test_data = {
            "dummy_value": [1, 2, 3],
            datetime_column_name: [1638646800000, 1638646860000, 1638646960000],
        }
        if unit == "s":
            test_data[datetime_column_name] = [
                timestamp // 1000 for timestamp in test_data[datetime_column_name]
            ]
        return pd.DataFrame(data=test_data)

    def test_reindex_on_datetime_milliseconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in milliseconds.
        """
        # Prepare inputs.
        dummy_df = self.get_dummy_df_with_timestamp()
        # Run.
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        # Check output.
        actual = str(reindexed_dummy_df)
        expected = (
            "                           dummy_value  dummy_timestamp\n"
            "dummy_timestamp                                        \n"
            "2021-12-04 19:40:00+00:00            1    1638646800000\n"
            "2021-12-04 19:41:00+00:00            2    1638646860000\n"
            "2021-12-04 19:42:40+00:00            3    1638646960000"
        )
        self.assert_equal(actual, expected)

    def test_reindex_on_datetime_seconds(self) -> None:
        """
        Verify datetime index creation when timestamp is in seconds.
        """
        # Prepare inputs.
        dummy_df = self.get_dummy_df_with_timestamp(unit="s")
        # Run.
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp", unit="s"
        )
        actual = str(reindexed_dummy_df)
        expected = (
            "                           dummy_value  dummy_timestamp\n"
            "dummy_timestamp                                        \n"
            "2021-12-04 19:40:00+00:00            1       1638646800\n"
            "2021-12-04 19:41:00+00:00            2       1638646860\n"
            "2021-12-04 19:42:40+00:00            3       1638646960"
        )
        self.assert_equal(actual, expected)

    def test_reindex_on_datetime_wrong_column(self) -> None:
        """
        Assert that wrong column is detected before reindexing.
        """
        dummy_df = self.get_dummy_df_with_timestamp()
        with self.assertRaises(AssertionError) as fail:
            imvcdttrut.reindex_on_datetime(dummy_df, "void_column")
        actual = str(fail.exception)
        expected = """
        * Failed assertion *
        'void_column' in 'Index(['dummy_value', 'dummy_timestamp'], dtype='object')'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_reindex_on_datetime_index_already_present(self) -> None:
        """
        Assert that reindexing is not done on already reindexed dataframe.
        """
        dummy_df = self.get_dummy_df_with_timestamp()
        reindexed_dummy_df = imvcdttrut.reindex_on_datetime(
            dummy_df, "dummy_timestamp"
        )
        with self.assertRaises(AssertionError) as fail:
            imvcdttrut.reindex_on_datetime(reindexed_dummy_df, "dummy")
        actual = str(fail.exception)
        expected = """
        * Failed assertion *
        'dummy' in 'Index(['dummy_value', 'dummy_timestamp'], dtype='object')'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################


class TestReindexOnCustomColumns(hunitest.TestCase):
    def get_test_data(self) -> pd.DataFrame:
        test_data = {
            "dummy_value_1": [1, 3, 2],
            "dummy_value_2": ["A", "C", "B"],
            "dummy_value_3": [0, 0, 0],
        }
        df = pd.DataFrame(data=test_data)
        df.index.name = "test"
        return df

    def test_reindex_on_custom_columns(self) -> None:
        """
        Verify that dataframe is re-indexed correctly with specified columns.
        """
        # Prepare inputs.
        test_data = self.get_test_data()
        expected_columns = test_data.columns.values.tolist()
        # Run.
        actual = imvcdttrut.reindex_on_custom_columns(
            test_data, expected_columns[:2], expected_columns
        )
        # Check output.
        actual = str(actual)
        expected = r"""                             dummy_value_3
        dummy_value_1 dummy_value_2
        1             A                          0
        2             B                          0
        3             C                          0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_reindex_on_custom_columns_invalid_columns(self) -> None:
        """
        Verify that dataframe is re-indexed correctly with specified columns.
        """
        # Prepare inputs.
        test_data = self.get_test_data()
        expected_columns = ["mock1", "mock2", "mock3"]
        # Run.
        with pytest.raises(AssertionError) as fail:
            imvcdttrut.reindex_on_custom_columns(
                test_data, expected_columns[:2], expected_columns
            )
        # Check output.
        actual = str(fail.value)
        expected = r"""
        * Failed assertion *
        val1=['mock1', 'mock2', 'mock3']
        issubset
        val2=['dummy_value_1', 'dummy_value_2', 'dummy_value_3']
        val1 - val2=['mock1', 'mock2', 'mock3']
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################


class TestTransformRawWebsocketData(hunitest.TestCase):
    def test_transform_raw_websocket_ohlcv_data(self) -> None:
        """
        Verify that raw OHLCV dict data received from websocket is transformed
        to DataFrame of specified format.
        """
        test_exchange = "binance"
        test_timestamp = pd.Timestamp("2022-10-05 15:06:00.019422+00:00")
        test_data = [
            {
                "ohlcv": [
                    [1664982180000, 1324.17, 1324.79, 1324.0, 1324.01, 2081.017],
                    [1664982240000, 1324.17, 1324.79, 1324.0, 1324.01, 2081.017],
                ],
                "currency_pair": "ETH/USDT",
                "end_download_timestamp": test_timestamp,
            },
            {
                "ohlcv": [
                    [1664982180000, 19900.0, 19913.2, 19900.0, 19905.1, 376.639],
                    [1664982240000, 19900.0, 19913.2, 19900.0, 19905.1, 376.639],
                ],
                "currency_pair": "BTC/USDT",
                "end_download_timestamp": test_timestamp,
            },
        ]
        expected_data = [
            [
                "ETH_USDT",
                1664982180000,
                1324.17,
                1324.79,
                1324.0,
                1324.01,
                2081.017,
                test_timestamp,
                test_exchange,
            ],
            [
                "ETH_USDT",
                1664982240000,
                1324.17,
                1324.79,
                1324.0,
                1324.01,
                2081.017,
                test_timestamp,
                test_exchange,
            ],
            [
                "BTC_USDT",
                1664982180000,
                19900.0,
                19913.2,
                19900.0,
                19905.1,
                376.639,
                test_timestamp,
                test_exchange,
            ],
            [
                "BTC_USDT",
                1664982240000,
                19900.0,
                19913.2,
                19900.0,
                19905.1,
                376.639,
                test_timestamp,
                test_exchange,
            ],
        ]
        expected_columns = [
            "currency_pair",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "end_download_timestamp",
            "exchange_id",
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)
        actual_df = imvcdttrut.transform_raw_websocket_data(
            test_data, "ohlcv", test_exchange
        ).reset_index(drop=True)
        self.assert_equal(
            hpandas.df_to_str(expected_df), hpandas.df_to_str(actual_df)
        )

    def test_transform_raw_websocket_bid_ask_data(self) -> None:
        """
        Verify that raw bid/ask dict data received from websocket is
        transformed to DataFrame of specified format.
        """
        test_exchange = "binance"
        test_timestamp = pd.Timestamp("2022-10-05 15:06:00.019422+00:00")
        test_data = [
            {
                "bids": [[1336.7, 60.789], [1336.69, 3.145]],
                "asks": [[1336.71, 129.483], [1336.72, 20.892]],
                "timestamp": 1664987681005,
                "datetime": "2022-10-05T16:34:41.005Z",
                "nonce": 2000450235897,
                "symbol": "ETH/USDT",
                "end_download_timestamp": test_timestamp,
            },
            {
                "bids": [[20066.5, 39.455], [20066.3, 1.346]],
                "asks": [[20066.6, 0.698], [20066.7, 0.008]],
                "timestamp": 1664987681001,
                "datetime": "2022-10-05T16:34:41.001Z",
                "nonce": 2000450235917,
                "symbol": "BTC/USDT",
                "end_download_timestamp": test_timestamp,
            },
        ]

        expected_data = [
            [
                "ETH_USDT",
                1664987681005,
                1336.7,
                60.789,
                1336.71,
                129.483,
                test_timestamp,
                1,
                test_exchange,
            ],
            [
                "ETH_USDT",
                1664987681005,
                1336.69,
                3.145,
                1336.72,
                20.892,
                test_timestamp,
                2,
                test_exchange,
            ],
            [
                "BTC_USDT",
                1664987681001,
                20066.5,
                39.455,
                20066.6,
                0.698,
                test_timestamp,
                1,
                test_exchange,
            ],
            [
                "BTC_USDT",
                1664987681001,
                20066.3,
                1.346,
                20066.7,
                0.008,
                test_timestamp,
                2,
                test_exchange,
            ],
        ]
        expected_columns = [
            "currency_pair",
            "timestamp",
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
            "end_download_timestamp",
            "level",
            "exchange_id",
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)
        actual_df = imvcdttrut.transform_raw_websocket_data(
            test_data, "bid_ask", test_exchange
        ).reset_index(drop=True)
        self.assert_equal(
            hpandas.df_to_str(expected_df), hpandas.df_to_str(actual_df)
        )