import argparse
import os
from typing import Any, Dict, Tuple

import helpers.hgit as hgit
import helpers.hsystem as hsysinte
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.convert_pq_by_date_to_by_asset as imvcdtcpbdtba


class TestPqByDateToByAsset1(hunitest.TestCase):
    @staticmethod
    def get_dummy_script_arguments() -> Dict[str, Any]:
        """
        Arguments used in script run.
        """
        return {
            "asset_col_name": "asset",
            # parallelization args
            "num_threads": "1",
            "dry_run": True,
            "no_incremental": True,
            "skip_on_error": True,
            "num_attempts": 1,
        }

    @staticmethod
    def get_dummy_task_config(test_dir: str) -> Dict[str, Any]:
        """
        Specific config used for joblib task.

        Along with regular arguments used in script run, there is
        `chunk` which represents list of daily PQ file paths that will
        be converted to by asset PQ files.
        """
        return {
            "src_dir": f"{test_dir}/by_date",
            "chunk": [
                f"{test_dir}/by_date/date=20211230/data.parquet",
                f"{test_dir}/by_date/date=20211231/data.parquet",
                f"{test_dir}/by_date/date=20220101/data.parquet",
            ],
            "dst_dir": f"{test_dir}/by_asset",
            "asset_col_name": "asset",
        }

    def generate_test_data(self, verbose: bool) -> Tuple[str, str]:
        """
        Generate test data in form of daily PQ files.
        """
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/test/generate_pq_example_data.py",
        )
        cmd.append(file_path)
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets A,B,C")
        cmd.append(f"--dst_dir {by_date_dir}")
        if verbose:
            cmd.append("--verbose")
            cmd.append("--reset_index")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        return test_dir, by_date_dir

    def check_directory_structure_with_file_contents(
        self, by_date_dir: str, by_asset_dir: str
    ) -> None:
        """
        Generate directory and file structure together with file contents in
        form of string. String is compared with previously generated .txt file
        for any differences.

        :param by_date_dir: daily PQ files before conversion
        :param by_asset_dir: daily PQ files after conversion in by asset format
        """
        include_file_content = True
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        actual = []
        actual.append("# by_date=")
        actual.append(by_date_signature)
        by_asset_signature = hunitest.get_dir_signature(
            by_asset_dir, include_file_content
        )
        actual.append("# by_asset=")
        actual.append(by_asset_signature)
        actual = "\n".join(actual)
        self.check_string(actual, purify_text=True)

    def test_daily_data2(self) -> None:
        verbose = True
        self._test_daily_data(verbose)

    def test_daily_data_direct_run1(self) -> None:
        verbose = False
        self._test_daily_data_direct_run(verbose)

    def test_daily_data_direct_run2(self) -> None:
        verbose = True
        self._test_daily_data_direct_run(verbose)

    def test__save_chunk(self) -> None:
        verbose = True
        self._test_joblib_task(verbose, {})

    def test_parser(self) -> None:
        """
        Tests arg parser for predefined args in the script.
        """
        parser = imvcdtcpbdtba._parse()
        cmd = []
        cmd.extend(["--src_dir", "dummy_by_date_dir"])
        cmd.extend(["--dst_dir", "dummy_by_asset_dir"])
        cmd.extend(["--num_threads", "1"])
        cmd.extend(["--asset_col_name", "ticker"])
        args = parser.parse_args(cmd)
        args = str(args).split("(")[-1]
        args = args.rstrip(")")
        args = args.split(", ")
        args = "\n".join(args)
        self.check_string(args, purify_text=True)

    def _test_daily_data(self, verbose: bool) -> None:
        """
        Generate daily data for 3 days in a by-date format and then convert it
        to by-asset.
        """
        test_dir, by_date_dir = self.generate_test_data(verbose)
        # Build command line to convert the data.
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/convert_pq_by_date_to_by_asset.py",
        )
        cmd.append(file_path)
        cmd.append(f"--src_dir {by_date_dir}")
        by_asset_dir = os.path.join(test_dir, "by_asset")
        cmd.append(f"--dst_dir {by_asset_dir}")
        cmd.append("--num_threads 1")
        if verbose:
            cmd.append("--asset_col_name ticker")
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        self.check_directory_structure_with_file_contents(
            by_date_dir, by_asset_dir
        )

    def _test_daily_data_direct_run(self, verbose: bool) -> None:
        """
        Tests directly _run function for coverage increase.

        Flow is same as in _test_daily_data.
        """
        test_dir, by_date_dir = self.generate_test_data(verbose)
        by_asset_dir = os.path.join(test_dir, "by_asset")
        kwargs = self.get_dummy_script_arguments()
        kwargs.update(
            {
                "src_dir": by_date_dir,
                "dst_dir": by_asset_dir,
            }
        )
        if verbose:
            kwargs.update(
                {
                    "asset_col_name": "ticker",
                }
            )
        args = argparse.Namespace(**kwargs)
        imvcdtcpbdtba._run(args)
        self.check_directory_structure_with_file_contents(
            by_date_dir, by_asset_dir
        )

    def _test_joblib_task(
        self, verbose: bool, config_update: Dict[str, Any]
    ) -> None:
        """
        Tests directly _save_chunk function that is used as joblib task.

        Because it is run as a separate process it is not caught by
        coverage.
        """
        test_dir, by_date_dir = self.generate_test_data(verbose)
        by_asset_dir = os.path.join(test_dir, "by_asset")
        config = self.get_dummy_task_config(test_dir)
        if verbose:
            config.update(
                {
                    "asset_col_name": "ticker",
                }
            )
        if config_update:
            config.update(config_update)
        imvcdtcpbdtba._save_chunk(**config)
        self.check_directory_structure_with_file_contents(
            by_date_dir, by_asset_dir
        )
