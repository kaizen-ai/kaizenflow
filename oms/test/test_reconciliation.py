import logging

import pytest
import helpers.hunit_test as hunitest
import oms.reconciliation as omreconc

_LOG = logging.getLogger(__name__)


class TestGetRunDate1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that function is correctly extracted from a start timestamp.
        """
        start_timestamp = "20231013_064500"
        act = omreconc.get_run_date(start_timestamp)
        exp = "20231013"
        self.assert_equal(act, exp)

    def test2(self) -> None:
        """
        Test that an exception is raised when the start timestamp is in 
        an invalid format.
        """
        start_timestamp = "20231013_12345678"
        with pytest.raises(Exception):
            omreconc.get_run_date(start_timestamp)