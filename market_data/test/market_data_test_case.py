"""
Import as:

import market_data.test.market_data_test_case as mdtmdtca
"""

import abc
import logging
from typing import Any, List, Optional, Union

import pandas as pd
import pytest

import helpers.hdatetime as hdateti
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import market_data as mdata

_LOG = logging.getLogger(__name__)


def _check_output(
    self_: Any,
    actual: Union[pd.DataFrame, pd.Series],
    expected_signature: str,
) -> None:
    """
    Verify that actual outcome matches the expected one.

    :param actual: actual outcome
    :param expected_signature: expected outcome as string
    """
    # Build signature.
    if isinstance(actual, pd.DataFrame):
        actual_signature = hpandas.df_to_str(
            actual, print_shape_info=True, tag="df"
        )
    elif isinstance(actual, pd.Series):
        actual_signature = hunitest.convert_df_to_string(
            actual, index=True, decimals=2
        )
    else:
        raise TypeError(
            f"Unsupported input type {type(actual)}. "
            f"Supported types are: `pd.DataFrame`, `pd.Series`"
        )
    _LOG.debug("\n%s", hpandas.df_to_str(actual))
    # Check.
    self_.assert_equal(
        actual_signature, expected_signature, dedent=True, fuzzy_match=True
    )


# #############################################################################
# MarketData_get_data_TestCase
# #############################################################################


class MarketData_get_data_TestCase(hunitest.TestCase, abc.ABC):
    """
    Test `get_data*()` methods for a class derived from `AbstractMarketData`.
    """

    @abc.abstractmethod
    def test_is_online1(self) -> None:
        """
        Test whether the DB is on-line at the current time.
        """
        ...

    # //////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _test_get_data_for_last_period(
        market_data: mdata.AbstractMarketData,
        timedelta: pd.Timestamp,
    ) -> None:
        """
        Call `get_data_for_last_period()` all conditional periods.

        This method is typically tested as smoke test, since it is a
        real-time method and we can't easily check the content of its
        output.
        """
        if skip_test_since_not_online(market_data):
            pytest.skip("Market not on-line")
        hprint.log_frame(
            _LOG,
            "get_data_for_last_period:" + hprint.to_str("timedelta"),
        )
        # Run.
        _ = market_data.get_data_for_last_period(timedelta)

    # //////////////////////////////////////////////////////////////////////////////

    def _test_get_data_at_timestamp1(
        self,
        market_data: mdata.AbstractMarketData,
        ts: pd.Timestamp,
        asset_ids: Optional[List[int]],
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_at_timestamp()` for specified parameters.
        """
        if skip_test_since_not_online(market_data):
            pytest.skip("Market not on-line")
        # Prepare inputs.
        ts_col_name = "end_ts"
        hprint.log_frame(
            _LOG,
            "get_data_at_timestamp:" + hprint.to_str("ts ts_col_name asset_ids"),
        )
        # Run.
        df = market_data.get_data_at_timestamp(ts, ts_col_name, asset_ids)
        # Check output.
        _check_output(self, df, exp_df_as_str)

    # //////////////////////////////////////////////////////////////////////////////

    def _get_data_for_interval_helper(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` for specified parameters.
        """
        if skip_test_since_not_online(market_data):
            pytest.skip("Market not on-line")
        # Prepare inputs.
        ts_col_name = "end_ts"
        hprint.log_frame(
            _LOG,
            "get_data_for_interval:"
            + hprint.to_str(
                "start_ts end_ts ts_col_name asset_ids left_close right_close"
            ),
        )
        # Run.
        df = market_data.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close=left_close,
            right_close=right_close,
        )
        # Check output.
        _check_output(self, df, exp_df_as_str)

    def _test_get_data_for_interval1(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` with:

        - asset_ids = None
        - interval type is default [a, b)
        """
        # Prepare inputs.
        asset_ids = None
        left_close = True
        right_close = False
        # Run.
        self._get_data_for_interval_helper(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            left_close,
            right_close,
            exp_df_as_str,
        )

    def _test_get_data_for_interval2(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: List[int],
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` with:

        - `asset_ids` is a list
        - interval type is default [a, b)
        """
        # Prepare inputs.
        left_close = True
        right_close = False
        # Run.
        self._get_data_for_interval_helper(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            left_close,
            right_close,
            exp_df_as_str,
        )

    def _test_get_data_for_interval3(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: List[int],
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` with:

        - `asset_ids` is a list
        - interval type is [a, b]
        """
        # Prepare inputs.
        left_close = True
        right_close = True
        # Run.
        self._get_data_for_interval_helper(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            left_close,
            right_close,
            exp_df_as_str,
        )

    def _test_get_data_for_interval4(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: List[int],
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` with:

        - `asset_ids` is a list
        - interval type is (a, b]
        """
        # Prepare inputs.
        left_close = False
        right_close = True
        # Run.
        self._get_data_for_interval_helper(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            left_close,
            right_close,
            exp_df_as_str,
        )

    def _test_get_data_for_interval5(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: List[int],
        exp_df_as_str: str,
    ) -> None:
        """
        Call `get_data_for_interval()` with:

        - `asset_ids` is a list
        - interval type is (a, b)
        """
        # Prepare inputs.
        left_close = False
        right_close = False
        # Run.
        self._get_data_for_interval_helper(
            market_data,
            start_ts,
            end_ts,
            asset_ids,
            left_close,
            right_close,
            exp_df_as_str,
        )

    # //////////////////////////////////////////////////////////////////////////////

    def _test_get_twap_price1(
        self,
        market_data: mdata.AbstractMarketData,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        asset_ids: Optional[List[int]],
        exp_srs_as_str: str,
    ) -> None:
        """
        Call `get_twap_price()` for specified parameters.
        """
        if skip_test_since_not_online(market_data):
            pytest.skip("Market not on-line")
        # Prepare inputs.
        ts_col_name = "end_ts"
        column = "close"
        hprint.log_frame(
            _LOG,
            "get_twap_price:"
            + hprint.to_str("start_ts end_ts ts_col_name asset_ids column"),
        )
        # Run.
        srs = market_data.get_twap_price(
            start_ts, end_ts, ts_col_name, asset_ids, column
        ).round(2)
        # Check output.
        _check_output(self, srs, exp_srs_as_str)

    # //////////////////////////////////////////////////////////////////////////////

    def _test_get_last_end_time1(
        self,
        market_data: mdata.AbstractMarketData,
        exp_last_end_time: pd.Timestamp,
    ) -> None:
        """
        Test that last end time is computed correctly.
        """
        # Run.
        act_last_end_time = market_data.get_last_end_time()
        # Check output.
        self.assertEqual(act_last_end_time, exp_last_end_time)

    def _test_get_last_price1(
        self,
        market_data: mdata.AbstractMarketData,
        asset_ids: Optional[List[int]],
        exp_srs_as_str: str,
    ) -> None:
        """
        Call `get_last_price()` for specified parameters.
        """
        if skip_test_since_not_online(market_data):
            pytest.skip("Market not on-line")
        # Prepare inputs.
        col_name = "close"
        hprint.log_frame(
            _LOG,
            "get_last_price:" + hprint.to_str("col_name asset_ids"),
        )
        # Run.
        srs = market_data.get_last_price(col_name, asset_ids).round(2)
        # Check output.
        _check_output(self, srs, exp_srs_as_str)

    # //////////////////////////////////////////////////////////////////////////////

    def _test_should_be_online1(
        self, market_data: mdata.AbstractMarketData, wall_clock_time: pd.Timestamp
    ) -> None:
        """
        Test that the interface is available at the given time.
        """
        # Run.
        actual = market_data.should_be_online(wall_clock_time)
        # Check output.
        self.assertTrue(actual)

    # TODO(GP): Implement test for `wait_for_latest_data()`.


def skip_test_since_not_online(market_data: mdata.AbstractMarketData) -> bool:
    """
    Return true if a test should be skipped since `market_data` is not on-line.
    """
    ret = False
    if not market_data.is_online():
        current_time = hdateti.get_current_time(tz="ET")
        _LOG.warning(
            "Skipping this test since DB is not on-line at %s", current_time
        )
        ret = True
    return ret


# #############################################################################
#
#
# class MarketData_get_data_for_last_period_asyncio_TestCase1(hunitest.TestCase):
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
#        tag = "get_data: " + hprint.to_str(
#            "wall_clock_time period normalize_data"
#        )
#        hprint.log_frame(_LOG, tag)
#        df = market_data.get_data_for_last_period(
#            period, normalize_data=normalize_data
#        )
#        act = hpandas.df_to_str(df, print_shape_info=True, tag=tag)
#        self.assert_equal(
#            act, exp_get_data_normalize_false, dedent=True, fuzzy_match=True
#        )
#        # Check `get_data(normalize=True)`.
#        normalize_data = True
#        tag = "get_data: " + hprint.to_str(
#            "wall_clock_time period normalize_data"
#        )
#        hprint.log_frame(_LOG, tag)
#        df = market_data.get_data_for_last_period(
#            period, normalize_data=normalize_data
#        )
#        act = hpandas.df_to_str(df, print_shape_info=True, tag=tag)
#        self.assert_equal(
#            act, exp_get_data_normalize_true, dedent=True, fuzzy_match=True
#        )
#
#    async def get_data_coroutine(self, event_loop) -> None:
#        # TODO(gp): Move this out.
#        # Build a `ReplayedMarketData`.
#        hprint.log_frame(_LOG, "ReplayedMarketData")
#        market_data = mdlime.get_ReplayedMarketData_example1(event_loop)
#        #
#        if skip_test_since_not_online(market_data):
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
#        # tag = "get_data:" + hprint.to_str("period normalize_data")
#        # hprint.log_frame(_LOG, tag)
#        # df = market_data.get_data(period, normalize_data=normalize_data)
#        # act = hpandas.df_to_str(df, print_shape_info=True, tag=tag)
#        # exp = ""
#        # self.assert_equal(act, exp)
#        # #
#        # ts = data["end_time"].max()
#        # normalize_data = False
#        # hprint.log_frame(_LOG, "get_data_at_timestamp:" + hprint.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#        # #
#        # normalize_data = True
#        # hprint.log_frame(_LOG, "get_data_at_timestamp:" + hprint.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#        # #
#        # ts = data["end_time"].min()
#        # normalize_data = False
#        # hprint.log_frame(_LOG, "get_data_at_timestamp:" + hprint.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#        # #
#        # normalize_data = True
#        # hprint.log_frame(_LOG, "get_data_at_timestamp:" + hprint.to_str("ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#        # #
#        # end_ts = data["end_time"].min()
#        # start_ts = end_ts - pd.DateOffset(minutes=5)
#        # ts_col_name = "start_time"
#        # normalize_data = False
#        # hprint.log_frame(_LOG, "get_data_for_timestamp:" + hprint.to_str("start_ts end_ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, start_ts, end_ts, ts_col_name, normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#        # #
#        # normalize_data = True
#        # hprint.log_frame(_LOG, "get_data_for_timestamp:" + hprint.to_str("start_ts end_ts normalize_data"))
#        # df = market_data.get_data_at_timestamp(ts, start_ts, end_ts, ts_col_name,
#        #                                                  normalize_data=normalize_data)
#        # _LOG.debug("\n%s", hpandas.df_to_str(df))
#
#    def test_get_data1(self) -> None:
#        with hasynci.solipsism_context() as event_loop:
#            coroutine = self.get_data_coroutine(event_loop)
#            hasynci.run(coroutine, event_loop=event_loop)
#
#
## TODO(gp): Build a ReplayedMarketData (e.g., from an example) and run the
##  MarketData tests on it.
