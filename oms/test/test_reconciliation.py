import os
from typing import Optional

import pandas as pd

import helpers.hio as hio
import helpers.hunit_test as hunitest
import oms.reconciliation as omreconc


class Test_get_system_run_parameters(hunitest.TestCase):
    def setUp(self) -> None:
        """
        Create test dirs that have prod-like structure.

        E.g.:
        ```
        C1b\
            paper_trading\
                20230620_131000.20230621_130500\
                    system_log_dir.scheduled
                20230620_131000.20230621_130500\
                    system_log_dir.scheduled
        ...
        ```
        """
        super().setUp()
        self._prod_data_root_dir = self.get_scratch_space()
        self._dag_builder_name = "C1b"
        self._run_mode = "paper_trading"
        system_run_params = [
            ("20230720_131000", "20230721_130500", "scheduled"),
            ("20230721_131000", "20230722_130500", "manual"),
            ("20230722_131000", "20230723_130500", "scheduled"),
            ("20230723_131000", "20230724_130500", "manual"),
            ("20230724_131000", "20230725_130500", "scheduled"),
        ]
        for (
            start_timestamp_as_str,
            end_timestamp_as_str,
            mode,
        ) in system_run_params:
            # Create target dir.
            target_dir = omreconc.get_target_dir(
                self._prod_data_root_dir,
                self._dag_builder_name,
                self._run_mode,
                start_timestamp_as_str,
                end_timestamp_as_str,
            )
            hio.create_dir(target_dir, incremental=True)
            # Create system log dir.
            system_log_dir = omreconc.get_prod_system_log_dir(mode)
            system_log_dir = os.path.join(target_dir, system_log_dir)
            hio.create_dir(system_log_dir, incremental=True)
        # Create multi-day test dirs.
        multiday_start_timestamp_as_str = "20230723_131000"
        multiday_end_timestamp_as_str = "20230730_130500"
        multiday_target_dir = omreconc.get_multiday_reconciliation_dir(
            self._prod_data_root_dir,
            self._dag_builder_name,
            self._run_mode,
            multiday_start_timestamp_as_str,
            multiday_end_timestamp_as_str,
        )
        hio.create_dir(multiday_target_dir, incremental=True)

    def check_helper(
        self,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        expected: str,
    ) -> None:
        """
        Compare the output with the expected result.
        """
        # Get all system run parameters.
        run_params = omreconc.get_system_run_parameters(
            self._prod_data_root_dir,
            self._dag_builder_name,
            self._run_mode,
            start_timestamp,
            end_timestamp,
        )
        # Check.
        actual = str(run_params)
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test1(self) -> None:
        """
        Test that system run parameters are returned in between
        `start_timestamp` and `end_timestamp`.
        """
        # Define params.
        start_timestamp = pd.Timestamp("2023-07-22 00:00:00+00:00")
        end_timestamp = pd.Timestamp("2023-07-24 23:59:59+00:00")
        expected = "[('20230722_131000', '20230723_130500', 'scheduled'), ('20230723_131000', '20230724_130500', 'manual'), ('20230724_131000', '20230725_130500', 'scheduled')]"
        self.check_helper(start_timestamp, end_timestamp, expected)

    def test2(self) -> None:
        """
        Test that returned system run parameters are filtered by
        `start_timestamp`.
        """
        # Define params.
        start_timestamp = pd.Timestamp("2023-07-21 00:00:00+00:00")
        end_timestamp = None
        expected = "[('20230721_131000', '20230722_130500', 'manual'), ('20230722_131000', '20230723_130500', 'scheduled'), ('20230723_131000', '20230724_130500', 'manual'), ('20230724_131000', '20230725_130500', 'scheduled')]"
        self.check_helper(start_timestamp, end_timestamp, expected)

    def test3(self) -> None:
        """
        Test that returned system run parameters are filtered by
        `end_timestamp`.
        """
        # Define params.
        start_timestamp = None
        end_timestamp = pd.Timestamp("2023-07-22 23:59:59+00:00")
        expected = "[('20230720_131000', '20230721_130500', 'scheduled'), ('20230721_131000', '20230722_130500', 'manual'), ('20230722_131000', '20230723_130500', 'scheduled')]"
        self.check_helper(start_timestamp, end_timestamp, expected)

    def test4(self) -> None:
        """
        Test that system run parameters are returned for all available dirs
        when `start_timestamp` and `end_timestamp` are not defined.
        """
        # Define params.
        start_timestamp = None
        end_timestamp = None
        expected = "[('20230720_131000', '20230721_130500', 'scheduled'), ('20230721_131000', '20230722_130500', 'manual'), ('20230722_131000', '20230723_130500', 'scheduled'), ('20230723_131000', '20230724_130500', 'manual'), ('20230724_131000', '20230725_130500', 'scheduled')]"
        self.check_helper(start_timestamp, end_timestamp, expected)
