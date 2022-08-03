import logging

import pandas as pd
import pytest

import im_v2.ccxt.data.client as icdcl
import market_data.market_data_example as mdmadaex
import market_data.test.market_data_test_case as mdtmdtca

# import market_data_lime.eg_market_data_example as mdlemdaex
# import market_data_lime.eg_stitched_market_data as mdlesmada

_LOG = logging.getLogger(__name__)


class TestStitchedMarketData1(mdtmdtca.MarketData_get_data_TestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    @pytest.mark.slow("~16 seconds by GH actions.")
    def test_get_data_for_interval5(self) -> None:
        # Prepare inputs.
        resample_1min = True
        dataset = "ohlcv"
        #
        universe_version1 = "v7"
        contract_type1 = "futures"
        data_snapshot1 = "20220707"
        im_client1 = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version1,
            resample_1min,
            dataset,
            contract_type1,
            data_snapshot1,
        )
        #
        universe_version2 = "v4"
        contract_type2 = "spot"
        data_snapshot2 = "20220530"
        im_client2 = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version2,
            resample_1min,
            dataset,
            contract_type2,
            data_snapshot2,
        )
        #
        asset_ids = [1467591036, 1464553467]
        columns = None
        column_remap = None
        wall_clock_time = pd.Timestamp("2022-05-10T00:00:01+00:00")
        filter_data_mode = "assert"
        market_data = mdmadaex.get_HorizontalStitchedMarketData_example1(
            im_client1,
            im_client2,
            asset_ids,
            columns,
            column_remap,
            wall_clock_time=wall_clock_time,
            filter_data_mode=filter_data_mode,
        )
        start_ts = pd.Timestamp("2022-05-01T00:01:00+00:00")
        end_ts = pd.Timestamp("2022-05-10T00:00:00+00:00")
        #
        expected_length = 25916
        expected_column_names = [
            "asset_id",
            "close_1",
            "close_2",
            "full_symbol",
            "high_1",
            "high_2",
            "knowledge_timestamp_1",
            "knowledge_timestamp_2",
            "low_1",
            "low_2",
            "open_1",
            "open_2",
            "start_ts",
            "volume_1",
            "volume_2",
        ]
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ETH_USDT"]
        }
        # pylint: disable=line-too-long
        exp_df_as_str = r"""
        # df=
        index=[2022-04-30 20:02:00-04:00, 2022-05-09 19:59:00-04:00]
        columns=asset_id,full_symbol,open_1,high_1,low_1,close_1,volume_1,knowledge_timestamp_1,start_ts,open_2,high_2,low_2,close_2,volume_2,knowledge_timestamp_2
        shape=(25916, 15)
                                    asset_id        full_symbol    open_1    high_1     low_1   close_1  volume_1            knowledge_timestamp_1                  start_ts    open_2    high_2     low_2   close_2    volume_2 knowledge_timestamp_2
        end_ts
        2022-04-30 20:02:00-04:00  1464553467  binance::ETH_USDT   2725.59   2730.42   2725.59   2730.04  1607.265 2022-06-24 11:10:10.287766+00:00 2022-04-30 20:01:00-04:00   2727.21   2731.74   2727.20   2731.67   563.15050            2022-05-10
        2022-04-30 20:02:00-04:00  1467591036  binance::BTC_USDT  37626.70  37667.20  37626.70  37658.80   321.075 2022-06-24 05:47:16.075108+00:00 2022-04-30 20:01:00-04:00  37642.28  37684.71  37642.28  37672.10    70.97044            2022-05-10
        2022-04-30 20:03:00-04:00  1464553467  binance::ETH_USDT   2730.04   2731.97   2725.33   2731.79  3056.801 2022-06-24 11:10:10.287766+00:00 2022-04-30 20:02:00-04:00   2731.67   2733.17   2727.18   2733.16  3325.93940            2022-05-10
        ...
        2022-05-09 19:58:00-04:00  1467591036  binance::BTC_USDT  30127.90  30206.30  30026.00  30137.30  2580.634 2022-06-24 05:47:16.075108+00:00 2022-05-09 19:57:00-04:00     NaN     NaN    NaN      NaN       NaN                   NaT
        2022-05-09 19:59:00-04:00  1464553467  binance::ETH_USDT   2235.59   2238.33   2224.34   2227.48  6847.862 2022-06-24 11:10:10.287766+00:00 2022-05-09 19:58:00-04:00     NaN     NaN    NaN      NaN       NaN                   NaT
        2022-05-09 19:59:00-04:00  1467591036  binance::BTC_USDT  30137.20  30175.10  30003.60  30056.60  1443.674 2022-07-09 12:07:51.240219+00:00 2022-05-09 19:58:00-04:00     NaN     NaN    NaN      NaN       NaN                   NaT
        """
        # pylint: enable=line-too-long
        # Run.
        self._test_get_data_for_interval5(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            exp_df_as_str,
        )

    def test_is_online1(self) -> None:
        self.assertTrue(True)


# class TestIgStitchedMarketData1(hunitest.TestCase):
#     def df_stats_to_str(self, df: pd.DataFrame) -> str:
#         txt = []
#         txt.append("min_date=%s" % df.index.min())
#         txt.append("max_date=%s" % df.index.max())
#         hdbg.dassert_in("asset_id", df.columns)
#         txt.append("asset_id=%s" % ",".join(map(str, df["asset_id"].unique())))
#         return "\n".join(txt)
#
#     def test_get_data_for_last_period1(self) -> None:
#         """
#         Get the data for the last 2 days for one EG id.
#         """
#         # TODO(gp): This test fails on Mondays. We need to extend the code to
#         #  support business days.
#         if datetime.date.today().weekday() == 0:
#             pytest.skip("Skip on Mondays")
#         asset_ids = [17085]
#         eg_stitched_market_data = mdlemdaex.get_IgStitchedMarketData_example1(
#             asset_ids
#         )
#         if mdtmdtca.skip_test_since_not_online(eg_stitched_market_data):
#             pytest.skip("Market not on-line")
#         # Query.
#         # timedelta = pd.tseries.offsets.BDay(2)
#         timedelta = pd.Timedelta("2D")
#         df = eg_stitched_market_data.get_data_for_last_period(timedelta)
#         _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="df"))
#         #
#         _LOG.info("result:\n%s", self.df_stats_to_str(df))
#
#     # TODO(gp): This is due to the slowdown in accessing S3 data.
#     @pytest.mark.superslow("20s")
#     def test_get_data_for_last_period2(self) -> None:
#         """
#         Get the data for the last 10 days for one EG id.
#         """
#         # TODO(gp): This test fails on Mondays. We need to extend the code to
#         #  support business days.
#         if datetime.date.today().weekday() == 0:
#             pytest.skip("Skip on Mondays")
#         asset_ids = [17085]
#         eg_stitched_market_data = mdlemdaex.get_IgStitchedMarketData_example1(
#             asset_ids
#         )
#         if mdtmdtca.skip_test_since_not_online(eg_stitched_market_data):
#             pytest.skip("Market not on-line")
#         # Query.
#         timedelta = pd.Timedelta("10D")
#         df = eg_stitched_market_data.get_data_for_last_period(timedelta)
#         _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="df"))
#         #
#         _LOG.info("result:\n%s", self.df_stats_to_str(df))
#
#     @pytest.mark.slow("20s")
#     def test_get_data_for_last_period3(self) -> None:
#         """
#         Get the data for the last 2 days for two EG ids.
#         """
#         # TODO(gp): This test fails on Mondays. We need to extend the code to
#         #  support business days.
#         if datetime.date.today().weekday() == 0:
#             pytest.skip("Skip on Mondays")
#         asset_ids = [17085, 13684]
#         eg_stitched_market_data = mdlemdaex.get_IgStitchedMarketData_example1(
#             asset_ids
#         )
#         if mdtmdtca.skip_test_since_not_online(eg_stitched_market_data):
#             pytest.skip("Market not on-line")
#         # Query.
#         # timedelta = pd.tseries.offsets.BDay(2)
#         timedelta = pd.Timedelta("2D")
#         df = eg_stitched_market_data.get_data_for_last_period(timedelta)
#         _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="df"))
#         #
#         _LOG.info("result:\n%s", self.df_stats_to_str(df))
#
#     @pytest.mark.slow("20s")
#     def test_get_data_for_last_period_compare1(self) -> None:
#         """
#         Retrieve 2 days worth of data from the RT market data and from the
#         stitched market data and compare it.
#         """
#         # TODO(gp): This test fails on Mondays. We need to extend the code to
#         #  support business days.
#         if datetime.date.today().weekday() == 0:
#             pytest.skip("Skip on Mondays")
#         asset_ids = [17085]
#         # Build the RT market data interface.
#         eg_rt_market_data = mdlemdaex.get_IgRealTimeMarketData_example1(asset_ids)
#         if mdtmdtca.skip_test_since_not_online(eg_rt_market_data):
#             pytest.skip("Market not on-line")
#         #
#         eg_stitched_market_data = mdlemdaex.get_IgStitchedMarketData_example1(
#             asset_ids
#         )
#         dst_dir = self.get_scratch_space()
#         # Query.
#         # timedelta = pd.tseries.offsets.BDay(2)
#         timedelta = pd.Timedelta("2D")
#         rt_df = eg_rt_market_data.get_data_for_last_period(timedelta)
#         rt_df.index = rt_df.index.tz_convert("America/New_York")
#         stitched_df = eg_stitched_market_data.get_data_for_last_period(timedelta)
#         stitched_df.index = stitched_df.index.tz_convert("America/New_York")
#         # Reorder the columns.
#         rt_df = mdlesmada.normalize_rt_df(rt_df)
#         # TODO(gp): Disabled due to missing `timestamp_db` in the historical flow.
#         # hdbg.dassert_set_eq(rt_df.columns, stitched_df.columns)
#         columns = sorted(rt_df)
#         rt_df = rt_df[columns]
#         stitched_df = stitched_df[columns]
#         # Save info for df_rt.
#         rt_df_as_str = hpandas.df_to_str(
#             rt_df, print_shape_info=True, tag="rt_df"
#         )
#         hio.to_file(os.path.join(dst_dir, "rt_df.txt"), rt_df_as_str)
#         rt_df.to_csv(os.path.join(dst_dir, "rt_df.csv"))
#         _LOG.debug("%s", rt_df_as_str)
#         # Save info for stitched_df.
#         stitched_df_as_str = hpandas.df_to_str(
#             stitched_df, print_shape_info=True, tag="stitched_df"
#         )
#         hio.to_file(os.path.join(dst_dir, "stitched_df.txt"), stitched_df_as_str)
#         stitched_df.to_csv(os.path.join(dst_dir, "stitched_df.csv"))
#         _LOG.debug("%s", stitched_df_as_str)
#         #
#         script_file_name = os.path.join(dst_dir, "/tmp.compare_stitched.sh")
#         txt = []
#         txt.append("vimdiff %s/rt_df.txt %s/stitched_df.txt" % (dst_dir, dst_dir))
#         txt.append("vimdiff %s/rt_df.csv %s/stitched_df.csv" % (dst_dir, dst_dir))
#         script_txt = "\n".join(txt)
#         hio.create_executable_script(script_file_name, script_txt)
#         _LOG.info("Compare with:\n%s", script_file_name)
