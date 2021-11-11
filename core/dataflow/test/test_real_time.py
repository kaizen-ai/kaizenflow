"""
Import as:

import core.dataflow.test.test_real_time as cdtfttrt
"""
import asyncio
import logging
from typing import Callable, Optional, Tuple

import pandas as pd
import pytest

import core.dataflow.real_time as cdtfretim
import helpers.datetime_ as hdatetim
import helpers.hasyncio as hhasynci
import helpers.htypes as hhtypes
import helpers.printing as hprintin
import helpers.timer as htimer
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


# #############################################################################


# The test code for a module needs to provide test objects to the rest of the
# testing code. This is to avoid recreating the same data structures everywhere
# leading to coupling.
# A potentially negative consequence of this approach is that test code needs
# to import other test code, which might need to turn testing code into Python
# package.


def get_test_data_builder1() -> Tuple[Callable, hhtypes.Kwargs]:
    """
    Return a data builder producing between "2010-01-04 09:30:00" and
    "2010-01-04 09:35:00" (for 5 minutes) every second.

    :return: `data_builder` and its kwargs for use inside a dataflow node.
    """
    data_builder = cdtfretim.generate_synthetic_data
    data_builder_kwargs = {
        "columns": ["close", "volume"],
        "start_datetime": pd.Timestamp("2010-01-04 09:30:00"),
        "end_datetime": pd.Timestamp("2010-01-05 09:30:00"),
        "freq": "1S",
        "seed": 42,
    }
    return data_builder, data_builder_kwargs


def get_test_data_builder2() -> Tuple[Callable, hhtypes.Kwargs]:
    """
    Return a data builder producing data between "2010-01-04 09:30:00" and
    "2010-01-04 09:30:05" (for 5 seconds) every second.

    :return: `data_builder` and its kwargs for use inside a dataflow node.
    """
    data_builder = cdtfretim.generate_synthetic_data
    data_builder_kwargs = {
        "columns": ["close", "volume"],
        "start_datetime": pd.Timestamp("2010-01-04 09:30:00"),
        "end_datetime": pd.Timestamp("2010-01-04 09:30:05"),
        "freq": "1S",
        "seed": 42,
    }
    return data_builder, data_builder_kwargs


# TODO(gp): -> _get_replayed_time? This should not be used.
# TODO(gp): Make `event_loop` mandatory.
def get_replayed_time(
    *,
    event_loop: Optional[asyncio.AbstractEventLoop] = None,
) -> cdtfretim.ReplayedTime:
    """
    Build a `ReplayedTime` object starting at the same time as the data (i.e.,
    "2010-01-04 09:30:00").
    """
    start_datetime = pd.Timestamp("2010-01-04 09:30:00")
    # Use a replayed real-time starting at the same time as the data.
    get_wall_clock_time = lambda: hdatetim.get_current_time(
        tz="naive_ET", event_loop=event_loop
    )
    rt = cdtfretim.ReplayedTime(start_datetime, get_wall_clock_time)
    return rt


# TODO(gp): Make `event_loop` mandatory.
def get_replayed_time_execute_rt_loop_kwargs(
    sleep_interval_in_secs: float,
    *,
    event_loop: Optional[asyncio.AbstractEventLoop] = None,
) -> hhtypes.Kwargs:
    """
    Return kwargs for a call to `execute_rt_loop` using replayed time.
    """
    # TODO(gp): Replace all these with `get_replayed_wall_clock_time()`.
    rt = get_replayed_time(event_loop=event_loop)
    get_wall_clock_time = rt.get_wall_clock_time
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        # TODO(gp): -> timeout everywhere
        "time_out_in_secs": 3.0 * sleep_interval_in_secs,
    }
    return execute_rt_loop_kwargs


def get_real_time_execute_rt_loop_kwargs(
    sleep_interval_in_secs: float,
    *,
    event_loop: Optional[asyncio.AbstractEventLoop],
) -> hhtypes.Kwargs:
    """
    Return kwargs for a call to `execute_rt_loop` using real time.
    """
    get_wall_clock_time = lambda: hdatetim.get_current_time(
        tz="naive_ET", event_loop=event_loop
    )
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        "time_out_in_secs": 3.0 * sleep_interval_in_secs,
    }
    return execute_rt_loop_kwargs


# #############################################################################


class Test_align_on_time_grid1(huntes.TestCase):
    """
    Test `align_on_time_grid` using different time semantics.
    """

    def helper(
        self,
        event_loop: Optional[asyncio.AbstractEventLoop],
        use_high_resolution: bool,
    ) -> None:
        """
        Run twice aligning on 2 seconds.
        """
        get_wall_clock_time = lambda: hdatetim.get_current_time(
            tz="ET", event_loop=event_loop
        )
        for i in range(2):
            _LOG.debug("\n%s", hprintin.frame("Iteration %s" % i, char1="="))
            current_time1 = get_wall_clock_time()
            _LOG.debug(hprintin.to_str("current_time1"))
            _ = current_time1
            # Align on an even second.
            grid_time_in_secs = 2
            cdtfretim.align_on_time_grid(
                get_wall_clock_time,
                grid_time_in_secs,
                event_loop=event_loop,
                use_high_resolution=use_high_resolution,
            )
            # Check.
            current_time2 = get_wall_clock_time()
            _LOG.debug(hprintin.to_str("current_time2"))
            self.assertEqual(current_time2.second % 2, 0)

    @pytest.mark.slow("It takes around 4 secs")
    def test_real_time1(self) -> None:
        """
        Test in real-time aligning on 2 seconds.
        """
        event_loop = None
        use_high_resolution = False
        self.helper(event_loop, use_high_resolution)

    @pytest.mark.slow("It takes around 4 secs")
    def test_real_time2(self) -> None:
        """
        Test in real-time aligning on 2 seconds.
        """
        event_loop = None
        use_high_resolution = True
        self.helper(event_loop, use_high_resolution)

    def test_replayed_time1(self) -> None:
        """
        Test in real-time aligning on 2 seconds.
        """
        use_high_resolution = False
        with hhasynci.solipsism_context() as event_loop:
            self.helper(event_loop, use_high_resolution)


# #############################################################################


class TestReplayedTime1(huntes.TestCase):
    def test1(self) -> None:
        """
        Rewind time to 9:30am of a day in the past.
        """
        rt = get_replayed_time()
        # We assume that these 2 calls take less than 1 minute.
        exp = pd.Timestamp("2010-01-04 09:30:00")
        self._helper(rt, exp)
        #
        self._helper(rt, exp)

    def _helper(self, rt: cdtfretim.ReplayedTime, exp: pd.Timestamp) -> None:
        rct = rt.get_wall_clock_time()
        _LOG.info("  -> time=%s", rct)
        _LOG.debug(hprintin.to_str("rct.date"))
        self.assert_equal(str(rct.date()), str(exp.date()))
        _LOG.debug(hprintin.to_str("rct.hour"))
        self.assert_equal(str(rct.hour), str(exp.hour))
        _LOG.debug(hprintin.to_str("rct.minute"))
        self.assert_equal(str(rct.minute), str(exp.minute))


# #############################################################################


class Test_execute_with_real_time_loop1(huntes.TestCase):
    """
    Run `execute_all_with_real_time_loop` with a workload that naively wait
    some time using different time semantics:

    - real time
    - simulated time
    - replayed time
    - simulated replayed time
    """

    @staticmethod
    async def workload(current_time: pd.Timestamp) -> bool:
        """
        Coroutine simulating a workload (waiting for 0.1s) when the current
        time has an even number of seconds.

        :return: True if it was executed
        """
        need_execute: bool = current_time.second % 2 == 0
        _LOG.debug(
            "current_time=%s -> need_execute=%s", current_time, need_execute
        )
        if need_execute:
            # The execution consists of just waiting.
            _LOG.debug("  -> execute")
            await asyncio.sleep(0.1)
        return need_execute

    def helper(
        self,
        get_wall_clock_time: hdatetim.GetWallClockTime,
        event_loop: Optional[asyncio.AbstractEventLoop],
    ) -> Tuple[str, str]:
        """
        Execute `_workload()` every second for 3 seconds using the passed event
        loops and wall clock times.

        :return: signature of the execution
        """
        # Align on a 2-second boundary, since we want to start always from an even
        # second.
        grid_time_in_secs = 2
        cdtfretim.align_on_time_grid(
            get_wall_clock_time, grid_time_in_secs, event_loop=event_loop
        )
        # Do 3 iterations of 1.0s.
        sleep_interval_in_secs = 1.0
        time_out_in_secs = 1.0 * 3 + 0.1
        #
        coroutine = cdtfretim.execute_all_with_real_time_loop(
            get_wall_clock_time,
            sleep_interval_in_secs,
            time_out_in_secs,
            self.workload,
        )
        events, results = hhasynci.run(coroutine, event_loop=event_loop)
        _LOG.debug("events=\n%s", str(events))
        _LOG.debug("results=\n%s", str(results))
        # Assemble output.
        events_as_str = "events=\n%s" % events.to_str(
            include_tenths_of_secs=False, include_wall_clock_time=False
        )
        results_as_str = "results=\n%s" % str(results)
        return events_as_str, results_as_str

    def replayed_time_helper(
        self, event_loop: Optional[asyncio.AbstractEventLoop]
    ) -> Tuple[str, str]:
        tz = "ET"
        initial_replayed_dt = pd.Timestamp(
            "2010-01-04 09:30:01", tz=hdatetim.get_ET_tz()
        )
        get_wall_clock_time = cdtfretim.get_replayed_wall_clock_time(
            tz, initial_replayed_dt, event_loop=event_loop
        )
        events_as_str, results_as_str = self.helper(
            get_wall_clock_time, event_loop
        )
        return events_as_str, results_as_str

    def check_output_real_time(
        self, events_as_str: str, results_as_str: str
    ) -> None:
        """
        Check the output when we run in real time.
        """
        # We can't check the events since they happen in real-time, so we check only
        # the results.
        _ = events_as_str
        exp = """
        results=
        [True, False, True]"""
        self.assert_equal(results_as_str, exp, dedent=True)

    def check_output_simulated(
        self, events_as_str: str, results_as_str: str
    ) -> None:
        """
        Check the output when we run in simulate time.
        """
        # Check that the events are triggered on even seconds.
        act = f"{events_as_str}\n{results_as_str}"
        exp = r"""
        events=
        num_it=1 current_time='2010-01-04 09:30:02-05:00'
        num_it=2 current_time='2010-01-04 09:30:03-05:00'
        num_it=3 current_time='2010-01-04 09:30:04-05:00'
        results=
        [True, False, True]"""
        self.assert_equal(act, exp, dedent=True)

    # #########################################################################

    def test_real_time1(self) -> None:
        """
        Use real-time semantic.

        We wait actual wall-clock time (without simulating the event
        loop).
        """
        # Run.
        event_loop = None
        get_wall_clock_time = lambda: hdatetim.get_current_time(tz="ET")
        events_as_str, results_as_str = self.helper(
            get_wall_clock_time, event_loop
        )
        # TODO(gp): This is flakey so we skip the check.
        # Check.
        # self._check_output_real_time(events_as_str, results_as_str)
        _ = events_as_str, results_as_str

    def test_simulated_time1(self) -> None:
        """
        Use simulated time semantic.

        We simulate real-time passing.
        """
        with htimer.TimedScope(logging.DEBUG, "") as ts:
            # Use the solipsistic event loop to simulate the real-time faster.
            with hhasynci.solipsism_context() as event_loop:
                # Use the wall clock time.
                get_wall_clock_time = lambda: hdatetim.get_current_time(
                    tz="ET", event_loop=event_loop
                )
                events_as_str, results_as_str = self.helper(
                    get_wall_clock_time, event_loop
                )
        # Check.
        self.check_output_real_time(events_as_str, results_as_str)
        # It should take less than 1 sec to simulate 4 secs.
        self.assertLess(ts.elapsed_time, 1)

    def test_replayed_time1(self) -> None:
        """
        Use replayed time semantic.

        We wait actual wall-clock time, but the time is rewinded to a
        point in the past.
        """
        event_loop = None
        events_as_str, results_as_str = self.replayed_time_helper(event_loop)
        # Check.
        self.check_output_simulated(events_as_str, results_as_str)

    def test_simulated_replayed_time1(self) -> None:
        """
        Use simulated replayed semantic.

        We simulate real-time passing in the past.
        """
        with htimer.TimedScope(logging.DEBUG, "") as ts:
            with hhasynci.solipsism_context() as event_loop:
                events_as_str, results_as_str = self.replayed_time_helper(
                    event_loop
                )
        # Check.
        self.check_output_simulated(events_as_str, results_as_str)
        # It should take less than 1 sec to simulate 4 secs.
        self.assertLess(ts.elapsed_time, 1)
