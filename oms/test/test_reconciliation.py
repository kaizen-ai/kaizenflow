import logging

import helpers.hunit_test as hunitest
import oms.reconciliation as omreconc

_LOG = logging.getLogger(__name__)


class TestGetRunDate(hunitest.TestCase):
    def test1(self) -> None:
        start_timestamp = "20231013_064500"
        act = omreconc.get_run_date(start_timestamp)
        exp = "20231013"
        self.assert_equal(act, exp)
