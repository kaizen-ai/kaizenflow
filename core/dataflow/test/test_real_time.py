import logging
import time

import pandas as pd
import pytest

import core.dataflow.real_time as cdrt
import helpers.datetime_ as hdatetime
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestReplayTime1(hut.TestCase):
    def _helper(self, rrt, exp: pd.Timestamp):
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s" % rct)
        _LOG.debug(hprint.to_str("rct.date"))
        self.assert_equal(str(rct.date()), str(exp.date()))
        _LOG.debug(hprint.to_str("rct.hour"))
        self.assert_equal(str(rct.hour), str(exp.hour))
        _LOG.debug(hprint.to_str("rct.minute"))
        self.assert_equal(str(rct.minute), str(exp.minute))

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
        _LOG.info("  -> time=%s" % rct)
        # We can't easily check the expected value, so we just check a lower bound.
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:01-04:00"))
        #
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s" % rct)
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:02-04:00"))


class Test_execute_dag_with_real_time_loop(hut.TestCase):
    @staticmethod
    def workload(current_time: pd.Timestamp) -> int:
        _ = current_time
        time.sleep(1)
        return 1

    def test_real_time1(self) -> None:
        """
        Test executing a workload every two seconds, starting from an even
        second using true wall clock time.
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
        execution_trace = cdrt.execute_with_real_time_loop(
            sleep_interval_in_secs,
            num_iterations,
            get_current_time,
            need_to_execute,
            self.workload,
        )
        # Check that the events are triggered every two seconds.
        act = "\n".join(map(str, execution_trace))
        exp = ""
        self.assert_equal(act, exp)

    def test_replayed_real_time1(self) -> None:
        """
        Test executing a workload every two seconds, starting from an even
        second using true wall clock time.
        """
        # Align on a even second.
        cdrt.align_on_even_second()
        #
        sleep_interval_in_secs = 1.0
        num_iterations = 3
        rrt = cdrt.ReplayRealTime(
            pd.Timestamp("2021-07-27 9:30:00", tz=hdatetime.get_ET_tz())
        )
        get_current_time = rrt.get_replayed_current_time
        need_to_execute = cdrt.execute_every_2_seconds
        #
        execution_trace = cdrt.execute_dag_with_real_time_loop(
            sleep_interval_in_secs,
            num_iterations,
            get_current_time,
            need_to_execute,
            self.workload,
        )
        # We can check that the times are exactly the expected ones, since we are
        # replaying time.
        act = "\n".join(map(str, execution_trace))
        exp = ""
        self.assert_equal(act, exp)

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
