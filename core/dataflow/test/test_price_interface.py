"""
Import as:

import core.dataflow.test.test_price_interface as dartttdi
"""

import asyncio
import logging
from typing import Any, Callable, List, Optional, Tuple

import numpy as np
import pandas as pd

import core.dataflow.price_interface as cdtfprint
import core.dataflow.real_time as cdtfretim
import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.hasyncio as hhasynci
import helpers.hnumpy as hhnumpy
import helpers.printing as hprintin
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


def generate_synthetic_db_data(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    columns: List[str],
    # TODO(gp): -> asset_ids
    ids: List[int],
    *,
    freq: str = "1T",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic data used to mimic real-time data.

    The data looks like:
    ```
    TODO(gp):
    ```
    """
    _LOG.debug(hprintin.to_str("start_datetime end_datetime columns ids freq seed"))
    hdatetim.dassert_tz_compatible(start_datetime, end_datetime)
    hdbg.dassert_lte(start_datetime, end_datetime)
    start_dates = pd.date_range(start_datetime, end_datetime, freq=freq)
    dfs = []
    for id_ in ids:
        df = pd.DataFrame()
        df["start_datetime"] = start_dates
        df["end_datetime"] = start_dates + pd.Timedelta(minutes=1)
        # TODO(gp): We can add 1 sec here to make it more interesting.
        df["timestamp_db"] = df["end_datetime"]
        # TODO(gp): Filter by ATH, if needed.
        # Random walk with increments independent and uniform in [-0.5, 0.5].
        for column in columns:
            with hhnumpy.random_seed_context(seed):
                data = np.random.rand(len(start_dates), 1) - 0.5  # type: ignore[var-annotated]
            df[column] = data.cumsum()
        df["asset_id"] = id_
        dfs.append(df)
    df = pd.concat(dfs, axis=0)
    return df


# TODO(gp): initial_replayed_delay -> initial_delay_in_mins (or in secs).
def get_replayed_time_price_interface_example1(
    event_loop: asyncio.AbstractEventLoop,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    initial_replayed_delay: int,
    delay_in_secs: int = 0,
    *,
    asset_ids: Optional[List[int]] = None,
    columns: Optional[List[str]] = None,
    df: Optional[pd.DataFrame] = None,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> cdtfprint.ReplayedTimePriceInterface:
    """
    Build a ReplayedTimePriceInterface backed by synthetic data.

    :param start_datetime: start time for the generation of the synthetic data
    :param end_datetime: end time for the generation of the synthetic data
    :param initial_replayed_delay: how many minutes after the beginning of the data
        the replayed time starts. This is useful to simulate the beginning / end of
        the trading day
    :param asset_ids: asset ids to generate data for. `None` defaults to asset_id=1000
    """
    # TODO(gp): This could / should be inferred from df.
    if asset_ids is None:
        asset_ids = [1000]
    # TODO(gp): Move it to the client, if possible.
    # Build the df with the data.
    if df is None:
        if columns is None:
            columns = ["last_price"]
        df = generate_synthetic_db_data(
            start_datetime, end_datetime, columns, asset_ids
        )
    # Build the `ReplayedTimePriceInterface` backed by the df with
    # `initial_replayed_dt` equal to a given number of minutes after the first
    # timestamp of the data.
    knowledge_datetime_col_name = "timestamp_db"
    asset_id_col_name = "asset_id"
    start_time_col_name = "start_datetime"
    end_time_col_name = "end_datetime"
    columns = None
    # Get the wall clock.
    tz = "ET"
    initial_replayed_dt = df[start_time_col_name].min() + pd.Timedelta(
        minutes=initial_replayed_delay
    )
    speed_up_factor = 1.0
    get_wall_clock_time = cdtfretim.get_replayed_wall_clock_time(
        tz,
        initial_replayed_dt,
        event_loop=event_loop,
        speed_up_factor=speed_up_factor,
    )
    # Build object.
    rtpi = cdtfprint.ReplayedTimePriceInterface(
        df,
        knowledge_datetime_col_name,
        delay_in_secs,
        #
        asset_id_col_name,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return rtpi


# #############################################################################


def _check_get_data(
    self_: Any,
    initial_replayed_delay: int,
    func: Callable,
    expected_df_as_str: str,
) -> cdtfprint.ReplayedTimePriceInterface:
    """
    - Build ReplayedTimePriceInterval
    - Execute `get_data*`
    - Check actual output against expected.
    """
    with hhasynci.solipsism_context() as event_loop:
        # Build ReplayedTimePriceInterval.
        start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
        end_datetime = pd.Timestamp("2000-01-01 10:29:00-05:00")
        rtpi = get_replayed_time_price_interface_example1(
            event_loop, start_datetime, end_datetime, initial_replayed_delay
        )
        # Execute function under test.
        actual_df = func(rtpi)
    # Check.
    actual_df = actual_df[sorted(actual_df.columns)]
    actual_df_as_str = hprintin.df_to_short_str("df", actual_df)
    _LOG.info("-> %s", actual_df_as_str)
    self_.assert_equal(
        actual_df_as_str,
        expected_df_as_str,
        dedent=True,
        fuzzy_match=True,
    )
    return rtpi


class TestReplayedTimePriceInterface1(huntes.TestCase):
    def check_last_end_time(
        self,
        rtpi: cdtfprint.ReplayedTimePriceInterface,
        expected_last_end_time: pd.Timestamp,
        expected_is_online: bool,
    ) -> None:
        """
        Check output of `get_last_end_time()` and `is_online()`.
        """
        #
        last_end_time = rtpi.get_last_end_time()
        _LOG.info("-> last_end_time=%s", last_end_time)
        self.assertEqual(last_end_time, expected_last_end_time)
        #
        is_online = rtpi.is_online()
        _LOG.info("-> is_online=%s", is_online)
        self.assertEqual(is_online, expected_is_online)

    def test_get_data1(self) -> None:
        """
        - Set the current time to 9:35
        - Get the last 5 mins of data
        - The returned data should be in [9:30, 9:35]
        """
        initial_replayed_delay = 5
        #
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:35:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00      1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        2000-01-01 09:32:00-05:00      1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        ...
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        2000-01-01 09:34:00-05:00      1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00      1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        expected_is_online = True
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data2(self) -> None:
        """
        Same as test_get_data1() but with normalize_data=False.
        """
        initial_replayed_delay = 5
        #
        period = "last_5mins"
        normalize_data = False
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [0, 4]
        df.columns=asset_id,end_datetime,last_price,start_datetime,timestamp_db
        df.shape=(5, 5)
           asset_id              end_datetime  last_price            start_datetime              timestamp_db
        0      1000 2000-01-01 09:31:00-05:00   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        1      1000 2000-01-01 09:32:00-05:00    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2      1000 2000-01-01 09:33:00-05:00    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        ...
        2      1000 2000-01-01 09:33:00-05:00    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        3      1000 2000-01-01 09:34:00-05:00    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        4      1000 2000-01-01 09:35:00-05:00    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        expected_is_online = True
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data_for_minute_0(self) -> None:
        """
        The replayed time starts at the same time of the data to represent the
        first minute of trading.
        """
        initial_replayed_delay = 0
        #
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # Check.
        expected_df_as_str = """
        # df=
        df.shape=(0, 4)
        Empty DataFrame
        Columns: [asset_id, last_price, start_datetime, timestamp_db]
        Index: []"""
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = None
        expected_is_online = False
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data_for_minute_1(self) -> None:
        """
        The replayed time starts one minute after the data to represent the
        first minute of trading.
        """
        initial_replayed_delay = 1
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        #
        expected_df_as_str = """
        # df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:31:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(1, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00      1000    -0.12546 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00"""
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:31:00-0500")
        expected_is_online = True
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data_for_minute_3(self) -> None:
        """
        The replayed time starts 3 minutes after the opening of the trading
        day.
        """
        initial_replayed_delay = 3
        #
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # Check.
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:33:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(3, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00      1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        2000-01-01 09:32:00-05:00      1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:33:00-05:00")
        expected_is_online = True
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data_for_minute_6(self) -> None:
        """
        The replayed time starts 6 minutes after the opening of the trading
        day.
        """
        initial_replayed_delay = 6
        #
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # Check.
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 09:32:00-05:00, 2000-01-01 09:36:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:32:00-05:00      1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        2000-01-01 09:34:00-05:00      1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        ...
        2000-01-01 09:34:00-05:00      1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00      1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00
        2000-01-01 09:36:00-05:00      1000   -0.032080 2000-01-01 09:35:00-05:00 2000-01-01 09:36:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:36:00-05:00")
        expected_is_online = True
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)

    def test_get_data_for_minute_63(self) -> None:
        """
        The replayed time starts 63 minutes after the opening of the trading
        day.
        """
        initial_replayed_delay = 63
        #
        period = "last_5mins"
        normalize_data = True
        func = lambda rtpi: rtpi.get_data(period, normalize_data=normalize_data)
        # Check.
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 10:29:00-05:00, 2000-01-01 10:30:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(2, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 10:29:00-05:00      1000   -1.775284 2000-01-01 10:28:00-05:00 2000-01-01 10:29:00-05:00
        2000-01-01 10:30:00-05:00      1000   -1.949954 2000-01-01 10:29:00-05:00 2000-01-01 10:30:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = _check_get_data(
            self, initial_replayed_delay, func, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 10:30:00-0500")
        expected_is_online = False
        self.check_last_end_time(rtpi, expected_last_end_time, expected_is_online)


# #############################################################################


class TestReplayedTimePriceInterface2(huntes.TestCase):

    # TODO(gp): Add same tests for the SQL version.
    def test_get_data_for_interval1(self) -> None:
        """
        - Start replaying time 5 minutes after the beginning of the day, i.e., the
          current time is 9:35.
        - Ask data for [9:30, 9:45]
        - The returned data is [9:30, 9:35].
        """
        # Start replaying time 5 minutes after the beginning of the day, so the
        # current time is 9:35.
        initial_replayed_delay = 5
        # Ask data for 9:30 to 9:45.
        start_ts = pd.Timestamp("2000-01-01 09:30:00-05:00")
        end_ts = pd.Timestamp("2000-01-01 09:45:00-05:00")
        ts_col_name = "end_datetime"
        asset_ids = None
        normalize_data = True
        func = lambda rtpi: rtpi.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids, normalize_data=normalize_data
        )
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:35:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00      1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        2000-01-01 09:32:00-05:00      1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        ...
        2000-01-01 09:33:00-05:00      1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        2000-01-01 09:34:00-05:00      1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00      1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00"""
        # pylint: enable=line-too-long
        _check_get_data(self, initial_replayed_delay, func, expected_df_as_str)

    def test_get_data_for_interval2(self) -> None:
        """
        - Current time is 9:45
        - Ask data in [9:35, 9:40]
        - The returned data is [9:30, 9:40].
        """
        initial_replayed_delay = 15
        start_ts = pd.Timestamp("2000-01-01 09:35:00-05:00")
        end_ts = pd.Timestamp("2000-01-01 09:40:00-05:00")
        ts_col_name = "start_datetime"
        asset_ids = None
        normalize_data = True
        func = lambda rtpi: rtpi.get_data_for_interval(
            start_ts, end_ts, ts_col_name, asset_ids, normalize_data=normalize_data
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        df.index in [2000-01-01 09:36:00-05:00, 2000-01-01 09:40:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:36:00-05:00      1000   -0.032080 2000-01-01 09:35:00-05:00 2000-01-01 09:36:00-05:00
        2000-01-01 09:37:00-05:00      1000   -0.473996 2000-01-01 09:36:00-05:00 2000-01-01 09:37:00-05:00
        2000-01-01 09:38:00-05:00      1000   -0.107820 2000-01-01 09:37:00-05:00 2000-01-01 09:38:00-05:00
        ...
        2000-01-01 09:38:00-05:00      1000   -0.107820 2000-01-01 09:37:00-05:00 2000-01-01 09:38:00-05:00
        2000-01-01 09:39:00-05:00      1000   -0.006705 2000-01-01 09:38:00-05:00 2000-01-01 09:39:00-05:00
        2000-01-01 09:40:00-05:00      1000    0.201367 2000-01-01 09:39:00-05:00 2000-01-01 09:40:00-05:00"""
        # pylint: enable=line-too-long
        _check_get_data(self, initial_replayed_delay, func, expected_df_as_str)

    def test_get_data_at_timestamp1(self) -> None:
        """
        - Current time is 9:45
        - Ask data for 9:35
        - The returned data is for 9:35
        """
        initial_replayed_delay = 15
        ts = pd.Timestamp("2000-01-01 09:35:00-05:00")
        ts_col_name = "start_datetime"
        asset_ids = None
        normalize_data = True
        func = lambda rtpi: rtpi.get_data_at_timestamp(
            ts, ts_col_name, asset_ids, normalize_data=normalize_data
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        df.index in [2000-01-01 09:36:00-05:00, 2000-01-01 09:36:00-05:00]
        df.columns=asset_id,last_price,start_datetime,timestamp_db
        df.shape=(1, 4)
                                   asset_id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:36:00-05:00      1000    -0.03208 2000-01-01 09:35:00-05:00 2000-01-01 09:36:00-05:00"""
        # pylint: enable=line-too-long
        _check_get_data(self, initial_replayed_delay, func, expected_df_as_str)

    def test_get_data_at_timestamp2(self) -> None:
        """
        - Current time is 9:45
        - Ask data for 9:50
        - The return data is empty
        """
        initial_replayed_delay = 15
        ts = pd.Timestamp("2000-01-01 09:50:00-05:00")
        ts_col_name = "start_datetime"
        asset_ids = None
        normalize_data = True
        func = lambda rtpi: rtpi.get_data_at_timestamp(
            ts, ts_col_name, asset_ids, normalize_data=normalize_data
        )
        # pylint: disable=line-too-long
        expected_df_as_str = r"""
        # df=
        df.shape=(0, 4)
        Empty DataFrame
        Columns: [asset_id, last_price, start_datetime, timestamp_db]
        Index: []"""
        # pylint: enable=line-too-long
        _check_get_data(self, initial_replayed_delay, func, expected_df_as_str)


# #############################################################################


class TestReplayedTimePriceInterface3(huntes.TestCase):
    """
    Test `ReplayedTimePriceInterface.is_last_bar_available()` using simulated
    time.
    """

    def test_get_last_end_time1(self) -> None:
        with hhasynci.solipsism_context() as event_loop:
            # Build object.
            start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
            end_datetime = pd.Timestamp("2000-01-01 10:30:00-05:00")
            initial_replayed_delay = 5
            delay_in_secs = 0
            rtpi = get_replayed_time_price_interface_example1(
                event_loop,
                start_datetime,
                end_datetime,
                initial_replayed_delay,
                delay_in_secs,
            )
            # Call method.
            last_end_time = rtpi.get_last_end_time()
        # Check.
        _LOG.info("-> last_end_time=%s", last_end_time)
        self.assertEqual(last_end_time, pd.Timestamp("2000-01-01 09:35:00-05:00"))

    # #########################################################################

    def test_is_last_bar_available1(self) -> None:
        """
        Wait for the market to open.
        """
        initial_replayed_delay = -2
        start_time, end_time, num_iter = self._run(initial_replayed_delay)
        # Check.
        expected_start_time = pd.Timestamp("2000-01-01 09:28:00-05:00")
        self.assertEqual(start_time, expected_start_time)
        #
        expected_end_time = pd.Timestamp("2000-01-01 09:31:00-05:00")
        self.assertEqual(end_time, expected_end_time)
        #
        expected_num_iter = 6
        self.assertEqual(num_iter, expected_num_iter)

    def test_is_last_bar_available2(self) -> None:
        """
        The market is already opened.
        """
        initial_replayed_delay = 5
        start_time, end_time, num_iter = self._run(initial_replayed_delay)
        # Check.
        expected_start_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        self.assertEqual(start_time, expected_start_time)
        #
        expected_end_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        self.assertEqual(end_time, expected_end_time)
        #
        expected_num_iter = 0
        self.assertEqual(num_iter, expected_num_iter)

    def test_is_last_bar_available3(self) -> None:
        """
        The market is closed, so we expect a timeout.
        """
        initial_replayed_delay = 63
        with self.assertRaises(TimeoutError):
            self._run(initial_replayed_delay)

    def _run(
        self, initial_replayed_delay: int
    ) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        """
        - Build a ReplayedTimePriceInterface
        - Run `is_last_bar_available()`
        """
        with hhasynci.solipsism_context() as event_loop:
            # Build a ReplayedTimePriceInterface.
            start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
            end_datetime = pd.Timestamp("2000-01-01 10:30:00-05:00")
            delay_in_secs = 0
            sleep_in_secs = 30
            time_out_in_secs = 60 * 5
            rtpi = get_replayed_time_price_interface_example1(
                event_loop,
                start_datetime,
                end_datetime,
                initial_replayed_delay,
                delay_in_secs,
                sleep_in_secs=sleep_in_secs,
                time_out_in_secs=time_out_in_secs,
            )
            # Run the method.
            start_time, end_time, num_iter = hhasynci.run(
                rtpi.is_last_bar_available(), event_loop=event_loop
            )
        return start_time, end_time, num_iter
