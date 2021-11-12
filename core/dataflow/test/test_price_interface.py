"""
Import as:

import dataflow_amp.real_time.test.test_price_interface as dartttdi
"""

import asyncio
import logging
from typing import List, Optional, Tuple

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
    *,
    freq: str = "1T",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic data used to mimic real-time data.
    """
    hdatetim.dassert_tz_compatible(start_datetime, end_datetime)
    hdbg.dassert_lte(start_datetime, end_datetime)
    start_dates = pd.date_range(start_datetime, end_datetime, freq=freq)
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
    df["id"] = 1000
    return df


# TODO(gp): initial_replayed_delay -> initial_replayed_delay_in_mins or
#  initial_delay_in_mins (or in secs).
def get_replayed_time_price_interface_example1(
    event_loop: asyncio.AbstractEventLoop,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    initial_replayed_delay: int,
    delay_in_secs: int = 0,
    *,
    df: Optional[pd.DataFrame] = None,
    sleep_in_secs: float = 1.0,
    time_out_in_secs: int = 60 * 2,
) -> cdtfprint.ReplayedTimePriceInterface:
    """
    :param initial_replayed_delay: how many minutes after the beginning of the data
        the replayed time starts. This is useful to simulate the beginning / end of
        the trading day
    """
    # Build the df with the data.
    if df is None:
        columns_ = ["last_price"]
        df = generate_synthetic_db_data(start_datetime, end_datetime, columns_)
    # Build the `ReplayedTimePriceInterface` backed by the df.
    id_col_name = "id"
    ids = [1000]
    start_time_col_name = "start_datetime"
    end_time_col_name = "end_datetime"
    knowledge_datetime_col_name = "timestamp_db"
    columns = None
    # Build a `ReplayedTime` with `initial_replayed_dt` equal to a given number of
    # minutes after the first timestamp of the data.
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
    #
    rtpi = cdtfprint.ReplayedTimePriceInterface(
        df,
        knowledge_datetime_col_name,
        delay_in_secs,
        #
        id_col_name,
        ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return rtpi


# #############################################################################


class TestReplayedTimePriceInterface1(huntes.TestCase):
    def test_get_last_end_time1(self) -> None:
        with hhasynci.solipsism_context() as event_loop:
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
            last_end_time = rtpi.get_last_end_time()
            _LOG.info("-> last_end_time=%s", last_end_time)
        self.assertEqual(last_end_time, pd.Timestamp("2000-01-01 09:35:00-05:00"))

    def test_get_data1(self) -> None:
        normalize_data = True
        initial_replayed_delay = 5
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:35:00-05:00]
        df.columns=id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                     id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00  1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        2000-01-01 09:32:00-05:00  1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        ...
        2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        2000-01-01 09:34:00-05:00  1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00  1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        expected_is_online = True
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data2(self) -> None:
        normalize_data = False
        initial_replayed_delay = 5
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [0, 4]
        df.columns=end_datetime,id,last_price,start_datetime,timestamp_db
        df.shape=(5, 5)
                       end_datetime    id  last_price            start_datetime              timestamp_db
        0 2000-01-01 09:31:00-05:00  1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        1 2000-01-01 09:32:00-05:00  1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2 2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        ...
        2 2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        3 2000-01-01 09:34:00-05:00  1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        4 2000-01-01 09:35:00-05:00  1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:35:00-05:00")
        expected_is_online = True
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data_for_minute_0(self) -> None:
        """
        The replayed time starts at the same time of the data to represent the
        first minute of trading.
        """
        normalize_data = True
        initial_replayed_delay = 0
        expected_df_as_str = """# df=
        df.shape=(0, 4)
        Empty DataFrame
        Columns: [id, last_price, start_datetime, timestamp_db]
        Index: []"""
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = None
        expected_is_online = False
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data_for_minute_1(self) -> None:
        """
        The replayed time starts one minute after the data to represent the
        first minute of trading.
        """
        normalize_data = True
        initial_replayed_delay = 1
        expected_df_as_str = """# df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:31:00-05:00]
        df.columns=id,last_price,start_datetime,timestamp_db
        df.shape=(1, 4)
        id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00  1000    -0.12546 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        """
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:31:00-0500")
        expected_is_online = True
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data_for_minute_3(self) -> None:
        """
        The replayed time starts 3 minutes after the opening of the trading
        day.
        """
        normalize_data = True
        initial_replayed_delay = 3
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 09:31:00-05:00, 2000-01-01 09:33:00-05:00]
        df.columns=id,last_price,start_datetime,timestamp_db
        df.shape=(3, 4)
                                     id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:31:00-05:00  1000   -0.125460 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00
        2000-01-01 09:32:00-05:00  1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:33:00-05:00")
        expected_is_online = True
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data_for_minute_6(self) -> None:
        """
        The replayed time starts 6 minutes after the opening of the trading
        day.
        """
        normalize_data = True
        initial_replayed_delay = 6
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 09:32:00-05:00, 2000-01-01 09:36:00-05:00]
        df.columns=id,last_price,start_datetime,timestamp_db
        df.shape=(5, 4)
                                     id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 09:32:00-05:00  1000    0.325254 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00
        2000-01-01 09:33:00-05:00  1000    0.557248 2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00
        2000-01-01 09:34:00-05:00  1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        ...
        2000-01-01 09:34:00-05:00  1000    0.655907 2000-01-01 09:33:00-05:00 2000-01-01 09:34:00-05:00
        2000-01-01 09:35:00-05:00  1000    0.311925 2000-01-01 09:34:00-05:00 2000-01-01 09:35:00-05:00
        2000-01-01 09:36:00-05:00  1000   -0.032080 2000-01-01 09:35:00-05:00 2000-01-01 09:36:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 09:36:00-05:00")
        expected_is_online = True
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def test_get_data_for_minute_63(self) -> None:
        """
        The replayed time starts 63 minutes after the opening of the trading
        day.
        """
        normalize_data = True
        initial_replayed_delay = 63
        # pylint: disable=line-too-long
        expected_df_as_str = """# df=
        df.index in [2000-01-01 10:29:00-05:00, 2000-01-01 10:30:00-05:00]
        df.columns=id,last_price,start_datetime,timestamp_db
        df.shape=(2, 4)
                                     id  last_price            start_datetime              timestamp_db
        end_datetime
        2000-01-01 10:29:00-05:00  1000   -1.775284 2000-01-01 10:28:00-05:00 2000-01-01 10:29:00-05:00
        2000-01-01 10:30:00-05:00  1000   -1.949954 2000-01-01 10:29:00-05:00 2000-01-01 10:30:00-05:00"""
        # pylint: enable=line-too-long
        rtpi = self._check_get_data(
            normalize_data, initial_replayed_delay, expected_df_as_str
        )
        #
        expected_last_end_time = pd.Timestamp("2000-01-01 10:30:00-0500")
        expected_is_online = False
        self._check_last_end_time(
            rtpi, expected_last_end_time, expected_is_online
        )

    def _check_get_data(
        self,
        normalize_data: bool,
        initial_replayed_delay: int,
        expected_df_as_str: str,
    ) -> cdtfprint.ReplayedTimePriceInterface:
        with hhasynci.solipsism_context() as event_loop:
            start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
            end_datetime = pd.Timestamp("2000-01-01 10:29:00-05:00")
            rtpi = get_replayed_time_price_interface_example1(
                event_loop, start_datetime, end_datetime, initial_replayed_delay
            )
            #
            period = "last_5mins"
            actual_df = rtpi.get_data(period, normalize_data=normalize_data)
            #
            actual_df = actual_df[sorted(actual_df.columns)]
            actual_df_as_str = hprintin.df_to_short_str("df", actual_df)
            _LOG.info("-> %s", actual_df_as_str)
            self.assert_equal(
                actual_df_as_str,
                expected_df_as_str,
                dedent=True,
                fuzzy_match=True,
            )
        return rtpi

    def _check_last_end_time(
        self,
        rtpi: cdtfprint.ReplayedTimePriceInterface,
        expected_last_end_time: pd.Timestamp,
        expected_is_online: bool,
    ) -> None:
        #
        last_end_time = rtpi.get_last_end_time()
        _LOG.info("-> last_end_time=%s", last_end_time)
        self.assertEqual(last_end_time, expected_last_end_time)
        #
        is_online = rtpi.is_online()
        _LOG.info("-> is_online=%s", is_online)
        self.assertEqual(is_online, expected_is_online)


# #############################################################################


class TestReplayedTimePriceInterface2(huntes.TestCase):
    def test_is_last_bar_available1(self) -> None:
        """
        Wait for the market to open.
        """
        initial_replayed_delay = -2
        start_time, end_time, num_iter = self._run(initial_replayed_delay)
        #
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
        #
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
        with hhasynci.solipsism_context() as event_loop:
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
            start_time, end_time, num_iter = hhasynci.run(
                rtpi.is_last_bar_available(), event_loop=event_loop
            )
        return start_time, end_time, num_iter
