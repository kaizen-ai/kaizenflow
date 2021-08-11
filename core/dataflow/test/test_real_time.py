"""
Import as:

import core.dataflow.test.test_real_time as cdtfttrt
"""
import logging
import time
from typing import Any, Callable, Tuple

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

import asyncio
#import async_solipsism

# def run(coroutine, loop) -> Any:
#     try:
#         ret = loop.run_until_complete(coroutine)
#     finally:
#         loop.close()
#     return ret

import helpers.hasyncio as hasyncio

class Test_execute_with_real_time_loop(hut.TestCase):
    @staticmethod
    async def workload(current_time: pd.Timestamp) -> bool:
        need_execute = cdrt.execute_every_2_seconds(current_time)
        _LOG.debug("current_time=%s -> need_execute=%s", current_time, need_execute)
        if need_execute:
            # The execution here is just waiting.
            _LOG.debug("  -> execute")
            await asyncio.sleep(0.1)
        return need_execute

    def helper(self, get_current_time, loop) -> str:
        """
        Test executing a workload every even second for 3 seconds using different
        event loops and wall clock times.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        # Do 3 iterations of 1.0s.
        sleep_interval_in_secs = 1.0
        time_out_in_secs = 1.0 * 3 + 0.1
        #
        events, results = hasyncio.run(
            cdrt.execute_with_real_time_loop(
                sleep_interval_in_secs,
                time_out_in_secs,
                get_current_time,
                self.workload,
            ),
            loop=loop
        )
        _LOG.debug("events=\n%s", str(events))
        _LOG.debug("results=\n%s", str(results))
        # Assemble output.
        txt = []
        txt.append("events=\n%s" % events.to_str(include_tenths_of_secs=False,
                                                 include_wall_clock_time=False))
        txt.append("results=\n%s" % str(results))
        txt = "\n".join(txt)
        _LOG.debug("%s", txt)
        return txt

    def test_real_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using true wall clock time.
        """
        get_current_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        loop = None
        _ = self.helper(get_current_time, loop)

    def test_simulated_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using true wall clock time.
        """
        get_current_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        with hasyncio.solipsism_context() as loop:
            act = self.helper(get_current_time, loop)
        # Check that the events are triggered every two seconds.
        exp = ""
        self.assert_equal(act, exp)

        # loop = async_solipsism.EventLoop()
        # asyncio.set_event_loop(loop)
        # # Align on an even second.
        # cdrt.align_on_even_second()
        # #
        # sleep_interval_in_secs = 1.0
        # time_out_in_secs = 3
        # # get_current_time = datetime.datetime.now
        # get_current_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        # #
        # events, results = run(
        #     cdrt.execute_with_real_time_loop(
        #         sleep_interval_in_secs,
        #         time_out_in_secs,
        #         get_current_time,
        #         self.workload,
        #     ), loop)
        # # TODO(gp): Check that the events are triggered every two seconds.
        # _ = events, results
        # _LOG.debug("events=\n%s", str(events))
        # _LOG.debug("results=\n%s", str(results))

    def test_replayed_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using replayed real time.
        """
        start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
        get_wall_clock_time = lambda: hdatetime.get_current_time(tz="ET")
        rrt = cdrt.ReplayRealTime(start_datetime, get_wall_clock_time)
        get_current_time = rrt.get_current_time
        loop = None
        act = self.helper(get_current_time, loop)
        # Check that the events are triggered every two seconds.
        exp = ""
        self.assert_equal(act, exp)

        # # Align on a even second.
        # cdrt.align_on_even_second()
        # #
        # execute_rt_loop_kwargs = get_test_execute_rt_loop_kwargs()
        # events, results = cdrt.execute_with_real_time_loop(
        #     **execute_rt_loop_kwargs,
        #     workload=self.workload,
        # )
        # _ = results
        # # We can check that the times are exactly the expected ones, since we are
        # # replaying time.
        # actual = "\n".join(
        #     [event.to_str(include_tenths_of_secs=False) for event in events]
        # )
        # # TODO(gp): Fix this. See AmpTask1618.
        # _ = actual
        # if False:
        #     expected = r"""
        #     num_it=1 current_time=20100104_093001 need_execute=False
        #     num_it=2 current_time=20100104_093002 need_execute=True
        #     num_it=3 current_time=20100104_093003 need_execute=False"""
        #     expected = hprint.dedent(expected)
        #     self.assert_equal(actual, expected)

    def test_simulated_replayed_time1(self) -> None:
        """
        Test executing a workload every second, starting from an even second
        using replayed real time.
        """
        get_wall_clock_time = lambda: hdatetime.get_current_time(tz="ET", loop=loop)
        start_datetime = pd.Timestamp("2010-01-04 09:30:00", tz=hdatetime.get_ET_tz())
        with hasyncio.solipsism_context() as loop:
            rrt = cdrt.ReplayRealTime(start_datetime, get_wall_clock_time)
            get_current_time = rrt.get_current_time
        act = self.helper(get_current_time, loop)
        # Check that the events are triggered every two seconds.
        exp = ""
        self.assert_equal(act, exp)

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
