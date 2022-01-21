from typing import List

import pandas as pd
import pytest

import market_data.market_data_client_example as mdmdclex
import market_data.test.market_data_test_case as mdtmdtca


# TODO(gp): There is a lot of common code among the test methods, let's factor it out.
class TestMarketDataImClient(mdtmdtca.MarketData_get_data_TestCase):
    # TODO(Dan): Resolve problem with get_wall_clock_time() in `AbstractMarketData`.
    @pytest.mark.skip(reason="Skip until TODO above is resolved")
    def test_get_data_for_last_period1(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        periods = (
            "last_day",
            "last_2days",
            "last_week",
            "last_10mins",
            "last_5mins",
            "last_1mins",
            "all",
        )
        normalize_data = True
        self._test_get_data_for_last_period1(market_data, periods, normalize_data)

    def test_get_data_at_timestamp1(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        normalize_data = True
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-16 20:05:00-04:00, 2018-08-16 20:05:00-04:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(2, 9)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:05:00-04:00  binance::BTC_USDT  6291.970000  6299.320000  6285.400000  6294.990000  18.986206      BTC_USDT     binance 2018-08-16 20:04:00-04:00
        2018-08-16 20:05:00-04:00   kucoin::ETH_USDT   285.400196   285.884637   285.400196   285.884637   0.006141      ETH_USDT      kucoin 2018-08-16 20:04:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_at_timestamp1(
            market_data, ts, asset_ids, normalize_data, exp_df_as_str
        )

    @pytest.mark.skip(
        reason="There are no files for the whole universe stored locally"
    )
    def test_get_data_for_interval1(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = None
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = """
        # df=
        df.index in [2018-08-16 20:01:00-04:00, 2018-08-16 20:04:00-04:00]
        df.columns=asset_id,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 9)
                                            asset_id         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:01:00-04:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206      BTC_USDT     binance 2018-08-16 20:00:00-04:00
        2018-08-16 20:01:00-04:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500      ETH_USDT      kucoin 2018-08-16 20:00:00-04:00
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        ...
        2018-08-16 20:03:00-04:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260      ETH_USDT      kucoin 2018-08-16 20:02:00-04:00
        2018-08-16 20:04:00-04:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586      BTC_USDT     binance 2018-08-16 20:03:00-04:00
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval1(
            market_data, start_ts, end_ts, exp_df_as_str
        )

    def test_get_data_for_interval2(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-16 20:01:00-04:00, 2018-08-16 20:04:00-04:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 9)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:01:00-04:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206      BTC_USDT     binance 2018-08-16 20:00:00-04:00
        2018-08-16 20:01:00-04:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500      ETH_USDT      kucoin 2018-08-16 20:00:00-04:00
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        ...
        2018-08-16 20:03:00-04:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260      ETH_USDT      kucoin 2018-08-16 20:02:00-04:00
        2018-08-16 20:04:00-04:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586      BTC_USDT     binance 2018-08-16 20:03:00-04:00
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval2(
            market_data, start_ts, end_ts, asset_ids, exp_df_as_str
        )

    def test_get_data_for_interval3(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id
        df.shape=(8, 8)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id
        timestamp
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206      BTC_USDT     binance
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500      ETH_USDT      kucoin
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260      ETH_USDT      kucoin
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586      BTC_USDT     binance
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval3(
            market_data, start_ts, end_ts, asset_ids, exp_df_as_str
        )

    def test_get_data_for_interval4(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-16 20:01:00-04:00, 2018-08-16 20:05:00-04:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(10, 9)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:01:00-04:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206      BTC_USDT     binance 2018-08-16 20:00:00-04:00
        2018-08-16 20:01:00-04:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500      ETH_USDT      kucoin 2018-08-16 20:00:00-04:00
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        ...
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00
        2018-08-16 20:05:00-04:00  binance::BTC_USDT  6291.970000  6299.320000  6285.400000  6294.990000  18.986206      BTC_USDT     binance 2018-08-16 20:04:00-04:00
        2018-08-16 20:05:00-04:00   kucoin::ETH_USDT   285.400196   285.884637   285.400196   285.884637   0.006141      ETH_USDT      kucoin 2018-08-16 20:04:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval4(
            market_data, start_ts, end_ts, asset_ids, exp_df_as_str
        )

    def test_get_data_for_interval5(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-16 20:02:00-04:00, 2018-08-16 20:05:00-04:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 9)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        2018-08-16 20:02:00-04:00   kucoin::ETH_USDT   286.405988   286.405988   285.400193   285.400197   0.162255      ETH_USDT      kucoin 2018-08-16 20:01:00-04:00
        2018-08-16 20:03:00-04:00  binance::BTC_USDT  6299.970000  6299.970000  6286.930000  6294.520000  34.611797      BTC_USDT     binance 2018-08-16 20:02:00-04:00
        ...
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00
        2018-08-16 20:05:00-04:00  binance::BTC_USDT  6291.970000  6299.320000  6285.400000  6294.990000  18.986206      BTC_USDT     binance 2018-08-16 20:04:00-04:00
        2018-08-16 20:05:00-04:00   kucoin::ETH_USDT   285.400196   285.884637   285.400196   285.884637   0.006141      ETH_USDT      kucoin 2018-08-16 20:04:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval5(
            market_data, start_ts, end_ts, asset_ids, exp_df_as_str
        )

    def test_get_data_for_interval6(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        # pylint: disable=line-too-long
        exp_df_as_str = r"""# df=
        df.index in [2018-08-16 20:02:00-04:00, 2018-08-16 20:04:00-04:00]
        df.columns=full_symbol,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(6, 9)
                                         full_symbol         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        2018-08-16 20:02:00-04:00   kucoin::ETH_USDT   286.405988   286.405988   285.400193   285.400197   0.162255      ETH_USDT      kucoin 2018-08-16 20:01:00-04:00
        2018-08-16 20:03:00-04:00  binance::BTC_USDT  6299.970000  6299.970000  6286.930000  6294.520000  34.611797      BTC_USDT     binance 2018-08-16 20:02:00-04:00
        ...
        2018-08-16 20:03:00-04:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260      ETH_USDT      kucoin 2018-08-16 20:02:00-04:00
        2018-08-16 20:04:00-04:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586      BTC_USDT     binance 2018-08-16 20:03:00-04:00
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00"""
        # pylint: enable=line-too-long
        self._test_get_data_for_interval6(
            market_data, start_ts, end_ts, asset_ids, exp_df_as_str
        )

    # //////////////////////////////////////////////////////////////////////////////

    def test_get_twap_price1(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        start_ts = pd.Timestamp("2018-08-17T00:01:00+00:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00+00:00")
        exp_srs_as_str = r"""
                            close
        full_symbol
        binance::BTC_USDT  6295.72
        kucoin::ETH_USDT    285.64"""
        self._test_get_twap_price1(
            market_data, start_ts, end_ts, asset_ids, exp_srs_as_str
        )

    # //////////////////////////////////////////////////////////////////////////////

    def test_should_be_online1(self) -> None:
        """
        See description of corresponding private method in parent class.
        """
        # Build MarketDataInterface.
        asset_ids = ["binance::BTC_USDT"]
        columns: List = []
        column_remap = None
        market_data = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Run test.
        wall_clock_time = pd.Timestamp("2018-08-17T00:01:00")
        self._test_should_be_online1(market_data, wall_clock_time)
