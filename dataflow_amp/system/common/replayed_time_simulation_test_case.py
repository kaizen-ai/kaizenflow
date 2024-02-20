"""
Import as:

import dataflow_amp.system.common.replayed_time_simulation_test_case as dtfascrtstc
"""

import os

import dataflow.system as dtfsys
import dataflow_amp.system.common.system_simulation_utils as dtfascssiut
import helpers.hgit as hgit
import helpers.hs3 as hs3
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest


# TODO(Grisha): can we re-use `Test_Time_ForecastSystem_TestCase1` somehow?
# The functionality looks similar to what the current test case does,
# we need to understand the differences and unify the test cases.
class Test_Replayed_Time_Simulation_TestCase(hunitest.TestCase):
    """
    Test case for replayed time simulation.
    """

    @staticmethod
    def _dump_market_data(
        dst_root_dir: str,
        start_timestamp_as_str: str,
        end_timestamp_as_str: str,
    ) -> None:
        """
        Dump market data to have an input for the simulation.

        See `amp/dataflow_amp/system/Cx/Cx_dump_market_data.py` for params
        description.
        """
        db_stage = "preprod"
        universe_version = "v7.4"
        opts = [
            f"--dst_dir {dst_root_dir}",
            f"--start_timestamp_as_str {start_timestamp_as_str}",
            f"--end_timestamp_as_str {end_timestamp_as_str}",
            f"--db_stage {db_stage}",
            f"--universe {universe_version}",
        ]
        # Save market data to the dir with the inputs.
        amp_dir = hgit.get_amp_abs_path()
        opts = " ".join(opts)
        script_name = os.path.join(
            amp_dir, "dataflow_amp", "system", "Cx", "Cx_dump_market_data.py"
        )
        cmd = " ".join([script_name, opts])
        hsystem.system(cmd, suppress_output=False)

    def _test_save_market_data(
        self, start_timestamp_as_str: str, end_timestamp_as_str: str
    ) -> None:
        """
        Save market data for unit testing.
        """
        scratch_dir = self.get_scratch_space()
        # Dump market data for a given interval.
        _ = self._dump_market_data(
            scratch_dir,
            start_timestamp_as_str,
            end_timestamp_as_str,
        )
        # Copy data to the test S3 input dir.
        market_data_file_name = "test_data.csv.gz"
        market_data_file_path = os.path.join(scratch_dir, market_data_file_name)
        s3_input_dir = self.get_s3_input_dir()
        aws_profile = "ck"
        hs3.copy_file_to_s3(market_data_file_path, s3_input_dir, aws_profile)

    def _test_run_simulation(
        self,
        system: dtfsys.System,
        start_timestamp_as_str: str,
        end_timestamp_as_str: str,
    ) -> None:
        """
        Run simulation.
        """
        # Copy market data file from S3 to a scratch dir.
        s3_input_dir = self.get_s3_input_dir()
        scratch_dir = self.get_scratch_space()
        aws_profile = "ck"
        hs3.copy_data_from_s3_to_local_dir(s3_input_dir, scratch_dir, aws_profile)
        # Define params for the simulation run.
        dst_system_log_dir = os.path.join(scratch_dir, "system_log_dir")
        market_data_file_name = "test_data.csv.gz"
        market_data_file_path = os.path.join(scratch_dir, market_data_file_name)
        config_tag = "prod_system"
        check_config = True
        # Run simulation.
        result_bundle = dtfascssiut.run_simulation(
            system,
            start_timestamp_as_str,
            end_timestamp_as_str,
            market_data_file_path,
            dst_system_log_dir,
            config_tag=config_tag,
            self_=self,
            check_config=check_config,
        )
        # Check the run signature.
        output_col_name = "vwap.ret_0.vol_adj.shift_-2_hat"
        result_bundle = result_bundle[-1]
        # TODO(Nina): freeze log dir structure. It requires to propagate
        # `get_wall_clock_time()` to a DAG. See CmTask5927 for reference.
        actual = dtfsys.get_signature(
            system.config, result_bundle, output_col_name
        )
        self.check_string(actual, fuzzy_match=True, purify_text=True)
