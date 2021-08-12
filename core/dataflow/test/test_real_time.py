"""
Import as:

import core.dataflow.test.test_real_time as cdtfttrt
"""
import asyncio
import logging
import time
from typing import Any, Callable, Tuple

import pandas as pd
import pytest

import core.dataflow.real_time as cdrt
import helpers.datetime_ as hdatetime
import helpers.hasyncio as hasyncio
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


def get_replayed_real_time() -> pd.Timestamp:
    start_datetime = pd.Timestamp("2010-01-04 09:30:00")
    # Use a replayed real-time starting at the same time as the data.
    rrt = cdrt.ReplayedTime(start_datetime, hdatetime.get_current_time(tz="NAIVE_ET"))
    get_current_time = rrt.get_replayed_current_time
    return get_current_time


# TODO(gp): Reduce to sleep_interval to 0.5 secs, if possible.
def get_test_execute_rt_loop_kwargs() -> htypes.Kwargs:
    get_wall_clock_time = get_replayed_real_time()
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": 1.0,
        "num_iterations": 3,
    }
    return execute_rt_loop_kwargs


# #############################################################################


class TestReplayedTime1(hut.TestCase):
    def test1(self) -> None:
        """
        Rewind time to 9:30am of a day in the past.
        """
        rrt = get_replayed_real_time()
        # We assume that these 2 calls take less than 1 minute.
        exp = pd.Timestamp("2010-01-04 09:30:00")
        self._helper(rrt, exp)
        #
        self._helper(rrt, exp)

    def _helper(self, rrt: cdrt.ReplayedTime, exp: pd.Timestamp) -> None:
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s", rct)
        _LOG.debug(hprint.to_str("rct.date"))
        self.assert_equal(str(rct.date()), str(exp.date()))
        _LOG.debug(hprint.to_str("rct.hour"))
        self.assert_equal(str(rct.hour), str(exp.hour))
        _LOG.debug(hprint.to_str("rct.minute"))
        self.assert_equal(str(rct.minute), str(exp.minute))


# #############################################################################


class Test_execute_with_real_time_loop1(hut.TestCase):
    @staticmethod
    async def workload(current_time: pd.Timestamp) -> bool:
        need_execute = cdrt.execute_every_2_seconds(current_time)
        _LOG.debug("current_time=%s -> need_execute=%s", current_time, need_execute)
        if need_execute:
            # The execution here is just waiting.
            _LOG.debug("  -> execute")
            await asyncio.sleep(0.1)
        return need_execute

    def helper(self, get_current_time: hdatetime.GetWallClockTime, loop: asyncio.AbstractEventLoop) -> Tuple[str, str]:
        """
        Test executing a workload every even second for 3 seconds using different
        event loops and wall clock times.
        """
        # Do 3 iterations of 1.0s.
        sleep_interval_in_secs = 1.0
        time_out_in_secs = 1.0 * 3 + 0.1
        #
        events, results = hasyncio.run(
            cdrt.execute_with_real_time_loop(
                get_current_time,
                sleep_interval_in_secs,
                time_out_in_secs,
                self.workload,
            ),
            loop=loop
        )
        _LOG.debug("events=\n%s", str(events))
        _LOG.debug("results=\n%s", str(results))
        # Assemble output.
        events_as_str = "events=\n%s" % events.to_str(include_tenths_of_secs=False,
                                                 include_wall_clock_time=False)
        results_as_str = "results=\n%s" % str(results)
        return events_as_str, results_as_str

    def test_real_time1(self) -> None:
        """
        Use real-time.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        # Use the wall clock time with no special event loop.
        get_current_time = lambda: hdatetime.get_current_time(tz="ET")
        loop = None
        events_as_str, results_as_str = self.helper(get_current_time, loop)
        # Check.
        self._check_output_real_time(events_as_str, results_as_str)

    def test_simulated_real_time1(self) -> None:
        """
        Use simulated real-time.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        # Use the wall clock time.
        get_current_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        # Use the solipsistic event loop to simulate the real-time faster.
        with hasyncio.solipsism_context() as loop:
            events_as_str, results_as_str = self.helper(get_current_time, loop)
        # Check.
        self._check_output_real_time(events_as_str, results_as_str)

    def _check_output_real_time(self, events_as_str: str, results_as_str: str) -> None:
        # We can't check the events since they happen in real-time, so we check only
        # the results.
        _ = events_as_str
        exp = """
        results=
        [True, False, True]"""
        self.assert_equal(results_as_str, exp, dedent=True)

    def test_replayed_real_time1(self) -> None:
        """
        Use replayed real-time.
        """
        # Create a replayed clock using the wall clock.
        start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
        get_wall_clock_time = lambda: hdatetime.get_current_time(tz="ET")
        rrt = cdrt.ReplayedTime(start_datetime, get_wall_clock_time)
        # Get replayed current time and no special loop (i.e., real-time).
        get_current_time = rrt.get_current_time
        loop = None
        events_as_str, results_as_str = self.helper(get_current_time, loop)
        # Check.
        self._check_output_replayed(events_as_str, results_as_str)

    def _check_output_replayed(self, events_as_str: str, results_as_str: str) -> None:
        # Check that the events are triggered on even seconds.
        act = f"{events_as_str}\n{results_as_str}"
        exp = r"""
        events=
        num_it=1 current_time='2010-01-04 09:30:00-05:00'
        num_it=2 current_time='2010-01-04 09:30:01-05:00'
        num_it=3 current_time='2010-01-04 09:30:02-05:00'
        results=
        [True, False, True]"""
        self.assert_equal(act, exp, dedent=True)

    def test_simulated_replayed_time1(self) -> None:
        """
        Use replayed simulated.
        """
        get_wall_clock_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
        with hasyncio.solipsism_context() as loop:
            rrt = cdrt.ReplayedTime(start_datetime, get_wall_clock_time)
            get_current_time = rrt.get_current_time
        events_as_str, results_as_str = self.helper(get_current_time, loop)
        # Check.
        self._check_output_replayed(events_as_str, results_as_str)


class Test_execute_with_real_time_loop2(hut.TestCase):

    @pytest.mark.slow("It takes around 4 secs")
    def test_align_on_even_second1(self) -> None:
        for _ in range(2):
            current_time1 = hdatetime.get_current_time(tz="ET")
            # Align on an even second.
            cdrt.align_on_even_second()
            # Check.
            current_time2 = hdatetime.get_current_time(tz="ET")
            _LOG.debug(hprint.to_str("current_time1 current_time2"))
            _ = current_time1
            self.assertEqual(current_time2.second % 2, 0)
