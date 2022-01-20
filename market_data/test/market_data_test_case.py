#import asyncio
#import logging
#import os
#
#import helpers.hasyncio as hasynci
#import helpers.hdatetime as hdatetim
#import helpers.hdbg as hdbg
#import helpers.hprint as hprintin
#import helpers.hunit_test as huntes
#import market_data as mdata
#import pandas as pd
#import pytest
#
#_LOG = logging.getLogger(__name__)
#
#
## #############################################################################
#
#
#class MarketData_get_data_TestCase(huntes.TestCase):
#    """
#    Test `get_data*()` methods for a class derived from `AbstractMarketData`.
#    """
#
#    def _test_get_data_for_last_period1(
#            self,
#            market_data: mdata.AbstractMarketData,
#            ) -> None:
#        """
#        Call `get_data_for_last_period()` for both values of `normalize_data`.
#
#        This method is typically tested as smoke test, since it is a real-time
#        method and we can't easily check the content of its output.
#        """
#        if mdata.skip_test_since_not_online(market_data):
#            pytest.skip("Market not on-line")
#        #
#        period = "last_10mins"
#        for normalize_data in (False, True):
#            hprintin.log_frame(
#                _LOG, "get_data_for_last_period:" + hprintin.to_str("period normalize_data")
#            )
#            df = market_data.get_data_for_last_period(
#                period, normalize_data=normalize_data
#            )
#            _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#
#    # //////////////////////////////////////////////////////////////////////////////
#
#    # TODO(gp): Add types.
#    # TODO(gp): ts -> timestamp
#    # TODO(gp): Pass the expected results like we do in other places of the code.
#    @staticmethod
#    def _get_data_at_timestamp_helper(market_data, ts):
#        ts_col_name = "end_time"
#        asset_ids = None
#        for normalize_data in (False, True):
#            hprintin.log_frame(
#                _LOG,
#                "get_data_at_timestamp:"
#                + hprintin.to_str("ts ts_col_name asset_ids normalize_data"),
#                )
#            df = market_data.get_data_at_timestamp(
#                ts, ts_col_name, asset_ids, normalize_data=normalize_data
#            )
#            _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#
#    # TODO(gp): Pass market_data to all the methods.
#    def _test_get_data_at_timestamp1(self):
#        if mdata.skip_test_since_not_online(market_data):
#            pytest.skip("Market not on-line")
#        ts = market_data._df["end_time"].min()
#        self.get_data_at_timestamp_helper(market_data, ts)
#
#    def _test_get_data_at_timestamp2(self):
#        market_data = self.get_RealTimeMarketData()
#        if mdata.skip_test_since_not_online(market_data):
#            pytest.skip("Market not on-line")
#        market_data = self.get_ReplayedMarketData(market_data)
#        ts = market_data._df["end_time"].max()
#        self.get_data_at_timestamp_helper(market_data, ts)
#
#    # //////////////////////////////////////////////////////////////////////////////
#
#    def _test_get_data_for_interval1(self) -> None:
#        if mdata.skip_test_since_not_online(market_data):
#            pytest.skip("Market not on-line")
#        end_ts = market_data._df["end_time"].max()
#        start_ts = end_ts - pd.DateOffset(minutes=5)
#        # start_ts = data["end_time"].min()
#        ts_col_name = "start_time"
#        asset_ids = None
#        for normalize_data in (False, True):
#            hprintin.log_frame(
#                _LOG,
#                "get_data_for_interval:"
#                + hprintin.to_str(
#                    "start_ts end_ts ts_col_name asset_ids normalize_data"
#                ),
#                )
#            df = market_data.get_data_for_interval(
#                start_ts,
#                end_ts,
#                ts_col_name,
#                asset_ids,
#                normalize_data=normalize_data,
#            )
#            _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#
#
## #############################################################################
#
#
#class MarketData_get_data_for_last_period_asyncio_TestCase1(huntes.TestCase):
#    """
#    Test `AbstractMarketData.get_data_for_last_period()` methods in an asyncio
#    set-up where time is moving forward.
#
#    This can only be tested with
#    """
#
#    def get_data_helper(
#        self,
#        market_data: mdata.AbstractMarketData,
#        get_wall_clock_time: hdatetime.GetWall,
#        exp_wall_clock_time: str,
#        exp_get_data_normalize_false: str,
#        exp_get_data_normalize_true: str,
#    ) -> None:
#        """
#        Check the output of data_
#
#        """
#        # TODO(gp): Rename get_data -> get_data_for_last_period
#        # Check the wall clock time.
#        wall_clock_time = get_wall_clock_time()
#        self.assert_equal(str(wall_clock_time), exp_wall_clock_time)
#        # Check `get_data(normalize=False)`.
#        period = "last_10mins"
#        normalize_data = False
#        tag = "get_data: " + hprintin.to_str(
#            "wall_clock_time period normalize_data"
#        )
#        hprintin.log_frame(_LOG, tag)
#        df = market_data.get_data_for_last_period(
#            period, normalize_data=normalize_data
#        )
#        act = hprintin.df_to_short_str(tag, df)
#        self.assert_equal(
#            act, exp_get_data_normalize_false, dedent=True, fuzzy_match=True
#        )
#        # Check `get_data(normalize=True)`.
#        normalize_data = True
#        tag = "get_data: " + hprintin.to_str(
#            "wall_clock_time period normalize_data"
#        )
#        hprintin.log_frame(_LOG, tag)
#        df = market_data.get_data_for_last_period(
#            period, normalize_data=normalize_data
#        )
#        act = hprintin.df_to_short_str(tag, df)
#        self.assert_equal(
#            act, exp_get_data_normalize_true, dedent=True, fuzzy_match=True
#        )
#
#    async def get_data_coroutine(self, event_loop) -> None:
#        # TODO(gp): Move this out.
#        # Build a `ReplayedMarketData`.
#        hprintin.log_frame(_LOG, "ReplayedMarketData")
#        market_data = mdlime.get_ReplayedMarketData_example1(event_loop)
#        #
#        if mdata.skip_test_since_not_online(market_data):
#            return
#        get_wall_clock_time = market_data.get_wall_clock_time
#        # We are at the beginning of the data.
#        exp_wall_clock_time = "2022-01-04 09:00:00-05:00"
#        # Since the clock is at the beginning of the day there is no data.
#        exp_get_data_normalize_false = r"""
#        # get_data: wall_clock_time=Timestamp('2022-01-04 09:00:00-0500', tz='America/New_York'), period='last_10mins', normalize_data=False=
#        df.shape=(0, 6)
#        Empty DataFrame
#        Columns: [egid, close, start_time, end_time, timestamp_db, volume]
#        Index: []"""
#        exp_get_data_normalize_true = r"""
#        # get_data: wall_clock_time=Timestamp('2022-01-04 09:00:00-0500', tz='America/New_York'), period='last_10mins', normalize_data=True=
#        df.shape=(0, 5)
#        Empty DataFrame
#        Columns: [egid, close, start_time, timestamp_db, volume]
#        Index: []"""
#        self.get_data_helper(
#            market_data,
#            get_wall_clock_time,
#            exp_wall_clock_time,
#            exp_get_data_normalize_false,
#            exp_get_data_normalize_true,
#        )
#        # - Wait 5 mins.
#        await asyncio.sleep(5 * 60)
#        exp_wall_clock_time = "2022-01-04 09:05:00-05:00"
#        exp_get_data_normalize_false = r"""
#        # get_data: wall_clock_time=Timestamp('2022-01-04 09:05:00-0500', tz='America/New_York'), period='last_10mins', normalize_data=False=
#        df.index in [4554, 4586]
#        df.columns=egid,close,start_time,end_time,timestamp_db,volume
#        df.shape=(8, 6)
#               egid  close                start_time                  end_time                     timestamp_db  volume
#        4586  13684    NaN 2022-01-04 09:00:00-05:00 2022-01-04 09:01:00-05:00 2022-01-04 09:01:05.142177-05:00       0
#        4582  17085    NaN 2022-01-04 09:00:00-05:00 2022-01-04 09:01:00-05:00 2022-01-04 09:01:05.142177-05:00       0
#        4576  13684    NaN 2022-01-04 09:01:00-05:00 2022-01-04 09:02:00-05:00 2022-01-04 09:02:02.832066-05:00       0
#        ...
#        4568  17085    NaN 2022-01-04 09:02:00-05:00 2022-01-04 09:03:00-05:00 2022-01-04 09:03:02.977737-05:00       0
#        4554  13684    NaN 2022-01-04 09:03:00-05:00 2022-01-04 09:04:00-05:00 2022-01-04 09:04:02.892229-05:00       0
#        4557  17085    NaN 2022-01-04 09:03:00-05:00 2022-01-04 09:04:00-05:00 2022-01-04 09:04:02.892229-05:00       0"""
#        exp_get_data_normalize_true = r"""
#        # get_data: wall_clock_time=Timestamp('2022-01-04 09:05:00-0500', tz='America/New_York'), period='last_10mins', normalize_data=True=
#        df.index in [2022-01-04 09:01:00-05:00, 2022-01-04 09:04:00-05:00]
#        df.columns=egid,close,start_time,timestamp_db,volume
#        df.shape=(8, 5)
#                                    egid  close                start_time                     timestamp_db  volume
#        end_time
#        2022-01-04 09:01:00-05:00  13684    NaN 2022-01-04 09:00:00-05:00 2022-01-04 09:01:05.142177-05:00       0
#        2022-01-04 09:01:00-05:00  17085    NaN 2022-01-04 09:00:00-05:00 2022-01-04 09:01:05.142177-05:00       0
#        2022-01-04 09:02:00-05:00  13684    NaN 2022-01-04 09:01:00-05:00 2022-01-04 09:02:02.832066-05:00       0
#        ...
#        2022-01-04 09:03:00-05:00  17085    NaN 2022-01-04 09:02:00-05:00 2022-01-04 09:03:02.977737-05:00       0
#        2022-01-04 09:04:00-05:00  13684    NaN 2022-01-04 09:03:00-05:00 2022-01-04 09:04:02.892229-05:00       0
#        2022-01-04 09:04:00-05:00  17085    NaN 2022-01-04 09:03:00-05:00 2022-01-04 09:04:02.892229-05:00       0"""
#        self.get_data_helper(
#            market_data,
#            get_wall_clock_time,
#            exp_wall_clock_time,
#            exp_get_data_normalize_false,
#            exp_get_data_normalize_true,
#        )
#
#        # TODO(gp): Add tests also for this.
#        # # - get_data()
#        # normalize_data = True
#        # tag = "get_data:" + hprintin.to_str("period normalize_data")
#        # hprintin.log_frame(_LOG, tag)
#        # df = market_data.get_data(period, normalize_data=normalize_data)
#        # act = hprintin.df_to_short_str(tag, df)
#        # exp = ""
#        # self.assert_equal(act, exp)
#        # #
#        # ts = data["end_time"].max()
#        # normalize_data = False
#        # hprintin.log_frame(_LOG, "get_data_at_timestamp:" + hprintin.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#        # #
#        # normalize_data = True
#        # hprintin.log_frame(_LOG, "get_data_at_timestamp:" + hprintin.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#        # #
#        # ts = data["end_time"].min()
#        # normalize_data = False
#        # hprintin.log_frame(_LOG, "get_data_at_timestamp:" + hprintin.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#        # #
#        # normalize_data = True
#        # hprintin.log_frame(_LOG, "get_data_at_timestamp:" + hprintin.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#        # #
#        # end_ts = data["end_time"].min()
#        # start_ts = end_ts - pd.DateOffset(minutes=5)
#        # ts_col_name = "start_time"
#        # normalize_data = False
#        # hprintin.log_frame(_LOG, "get_data_for_timestamp:" + hprintin.to_str("start_ts end_ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, start_ts, end_ts, ts_col_name, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#        # #
#        # normalize_data = True
#        # hprintin.log_frame(_LOG, "get_data_for_timestamp:" + hprintin.to_str("start_ts end_ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, start_ts, end_ts, ts_col_name,
#        #                                                  normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hprintin.dataframe_to_str(df))
#
#    def test_get_data1(self) -> None:
#        with hasynci.solipsism_context() as event_loop:
#            coroutine = self.get_data_coroutine(event_loop)
#            hasynci.run(coroutine, event_loop=event_loop)
#
#
## TODO(gp): Build a ReplayedMarketData (e.g., from an example) and run the
##  MarketData tests on it.
