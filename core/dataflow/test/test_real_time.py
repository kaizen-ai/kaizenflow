import logging
import os
from typing import Any

import pandas as pd

import helpers.printing as hprint
import core.dataflow.real_time as cdrt
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
        rrt = cdrt.ReplayRealTime(pd.Timestamp("2021-07-27 9:30:00-04:00"),
                                  speed_up_factor=1e6)
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s" % rct)
        # We can't easily check the expected value, so we just check a lower bound.
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:01-04:00"))
        #
        rct = rrt.get_replayed_current_time()
        _LOG.info("  -> time=%s" % rct)
        self.assertGreater(rct, pd.Timestamp("2021-07-27 9:30:02-04:00"))


class Test_execute_dag_with_real_time_loop(hut.TestCase):

    def test_real_time(self) -> None:
        sleep_interval_in_secs = 1.0
        num_iterations = 10
        #get_current_time = datetime.datetime.now
        get_current_time = lambda : hdatetime.get_current_time(tz="ET")
        need_to_execute = cdrt.execute_every_5_minutes
        cdrt.execute_dag_with_real_time_loop(sleep_interval_in_secs,
                                             num_iterations,
                                             get_current_time,
                                             need_to_execute)

    def test_real_time(self) -> None:
        sleep_interval_in_secs = 1.0
        num_iterations = 10
        get_current_time = rrt.get_replayed_current_time
        need_to_execute = cdrt.execute_every_5_minutes
        cdrt.execute_dag_with_real_time_loop(sleep_interval_in_secs,
                                             num_iterations,
                                             get_current_time,
                                             need_to_execute)

