"""
Import as:

import core.dataflow.test.test_real_time as cdtfttrt
"""
import logging
import time
from typing import Callable, Tuple

import async_solipsism
import pandas as pd
import pytest

import core.dataflow.real_time as cdrt
import helpers.datetime_ as hdatetime
import helpers.htypes as htypes
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# The test code for a module needs to provide test objects to the rest of the testing
# code. This is to avoid recreating the same data structures everywhere leading to
# coupling.
# A potentially negative consequence of this approach is that test code needs to
# include test code, which might need to turn testing code into Python package.


def get_test_data_builder1() -> Tuple[Callable, htypes.Kwargs]:
    """
    Return data between "2010-01-04 09:30:00" and "2010-01-05 09:30:00".
    """
    data_builder = cdrt.generate_synthetic_data
    data_builder_kwargs = {
        "columns": ["close", "volume"],
        "start_datetime": pd.Timestamp("2010-01-04 09:30:00"),
        "end_datetime": pd.Timestamp("2010-01-05 09:30:00"),
        "seed": 42,
    }
    return data_builder, data_builder_kwargs


def get_test_data_builder2() -> Tuple[Callable, htypes.Kwargs]:
    """
    Return data between "2010-01-04 09:30:00" and "2010-01-04 09:35:00".
    """
    data_builder = cdrt.generate_synthetic_data
    data_builder_kwargs = {
        "columns": ["close", "volume"],
        "start_datetime": pd.Timestamp("2010-01-04 09:30:00"),
        "end_datetime": pd.Timestamp("2010-01-04 09:35:00"),
        "seed": 42,
    }
    return data_builder, data_builder_kwargs


def get_test_current_time() -> pd.Timestamp:
    start_datetime = pd.Timestamp("2010-01-04 09:30:00")
    # Use a replayed real-time starting at the same time as the data.
    rrt = cdrt.ReplayRealTime(start_datetime)
    get_current_time = rrt.get_replayed_current_time
    return get_current_time


# TODO(gp): Reduce to sleep_interval to 0.5 secs.
def get_test_execute_rt_loop_kwargs() -> htypes.Kwargs:
    get_current_time = get_test_current_time()
    execute_rt_loop_kwargs = {
        "sleep_interval_in_secs": 1.0,
        "num_iterations": 3,
        "get_current_time": get_current_time,
        "need_to_execute": cdrt.execute_every_2_seconds,
    }
    return execute_rt_loop_kwargs


# #############################################################################


class TestReplayTime1(hut.TestCase):
    def test1(self) -> None:
        """
        Rewind time to 9:30am of a day in the past.
        """
        rrt = cdrt.ReplayRealTime(pd.Timestamp("2021-07-27 9:30:00-04:00"))
        # We assume that these 2 calls take less than 1 minute.
        exp = pd.Timestamp("2021-07-27 9:30")
        self._helper(rrt, exp)
        #
        exp = pd.Timestamp("2021-07-27 9:30")
        self._helper(rrt, exp)

    def test2(self) -> None:
        """
        Rewind time to 9:30am of a day in the past and speed up time 1e6 times.
        """
        rrt = cdrt.ReplayRealTime(
            pd.Timestamp("2021-07-27 9:30:00-04:00"), speed_up_factor=1e6
        )
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s", rct)
        # We can't easily check the expected value, so we just check a lower bound.
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:01-04:00"))
        #
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s", rct)
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:02-04:00"))

    def _helper(self, rrt: cdrt.ReplayRealTime, exp: pd.Timestamp) -> None:
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s", rct)
        _LOG.debug(hprint.to_str("rct.date"))
        self.assert_equal(str(rct.date()), str(exp.date()))
        _LOG.debug(hprint.to_str("rct.hour"))
        self.assert_equal(str(rct.hour), str(exp.hour))
        _LOG.debug(hprint.to_str("rct.minute"))
        self.assert_equal(str(rct.minute), str(exp.minute))


# #############################################################################


class Test_execute_with_real_time_loop(hut.TestCase):
    @staticmethod
    def workload(current_time: pd.Timestamp) -> int:
        _ = current_time
        time.sleep(0.3)
        return 1

    def test_real_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using true wall clock time.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        #
        sleep_interval_in_secs = 1.0
        num_iterations = 3
        # get_current_time = datetime.datetime.now
        get_current_time = lambda: hdatetime.get_current_time(tz="ET")
        need_to_execute = cdrt.execute_every_2_seconds
        #
        events, results = cdrt.execute_with_real_time_loop(
            sleep_interval_in_secs,
            num_iterations,
            get_current_time,
            need_to_execute,
            self.workload,
        )
        # TODO(gp): Check that the events are triggered every two seconds.
        _ = events, results

    def test_replayed_real_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using replayed real time.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        #
        execute_rt_loop_kwargs = get_test_execute_rt_loop_kwargs()
        events, results = cdrt.execute_with_real_time_loop(
            **execute_rt_loop_kwargs,
            workload=self.workload,
        )
        _ = results
        # We can check that the times are exactly the expected ones, since we are
        # replaying time.
        actual = "\n".join(
            [event.to_str(include_tenths_of_secs=False) for event in events]
        )
        # TODO(gp): Fix this. See AmpTask1618.
        _ = actual
        if False:
            expected = r"""
            num_it=1 current_time=20100104_093001 need_execute=False
            num_it=2 current_time=20100104_093002 need_execute=True
            num_it=3 current_time=20100104_093003 need_execute=False"""
            expected = hprint.dedent(expected)
            self.assert_equal(actual, expected)

    @pytest.mark.slow("It takes around 6 secs")
    def test_align_on_even_second1(self) -> None:
        for _ in range(3):
            current_time1 = hdatetime.get_current_time(tz="ET")
            # Align on an even second.
            cdrt.align_on_even_second()
            # Check.
            current_time2 = hdatetime.get_current_time(tz="ET")
            _LOG.debug(hprint.to_str("current_time1 current_time2"))
            _ = current_time1
            self.assertEqual(current_time2.second % 2, 0)
