import argparse
import os
from typing import Dict, List, Tuple

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest

from . import generate_pq_example_data


class TestGeneratePqExampleData1(hunitest.TestCase):
    def generate_test_data(self, verbose: bool) -> str:
        """
        Generate test data in form of daily PQ files.
        """
        by_date_dir, file_path = self._prepare_scratch_space()
        cmd = self._prepare_command_args(by_date_dir, verbose)
        # Insert file path as first command argument.
        cmd.insert(0, file_path)
        cmd = " ".join(cmd)
        hsysinte.system(cmd)
        return by_date_dir

    def check_directory_structure_with_file_contents(
        self, by_date_dir: str
    ) -> None:
        """
        Generate directory and file structure together with file contents in
        form of string. String is compared with previously generated .txt file
        for any differences.

        :param by_date_dir: location of generated daily PQ files
        """
        include_file_content = True
        by_date_signature = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        actual = []
        actual.append("# by_date=")
        actual.append(by_date_signature)
        actual = "\n".join(actual)
        purify_text = True
        self.check_string(actual, purify_text=purify_text)

    @staticmethod
    def _prepare_command_args(by_date_dir: str, verbose: bool) -> List[str]:
        cmd = []
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets A,B,C")
        cmd.append(f"--dst_dir {by_date_dir}")
        if verbose:
            cmd.append("--verbose")
        return cmd

    def _prepare_scratch_space(self) -> Tuple[str, str]:
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/test/generate_pq_example_data.py",
        )
        return by_date_dir, file_path

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_generate_example_data1(self) -> None:
        """
        Generate daily data for 3 days in basic by-date format.
        """
        verbose = False
        by_date_dir = self.generate_test_data(verbose)
        self.check_directory_structure_with_file_contents(by_date_dir)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_generate_example_data2(self) -> None:
        """
        Generate daily data for 3 days in verbose by-date format.
        """
        verbose = True
        by_date_dir = self.generate_test_data(verbose)
        self.check_directory_structure_with_file_contents(by_date_dir)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_generate_example_data_direct_run1(self) -> None:
        """
        Tests directly _run function for coverage increase.

        Flow is same as in _test_generate_example_data1.
        """
        verbose = False
        self._direct_run(verbose)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_generate_example_data_direct_run2(self) -> None:
        """
        Tests directly _run function for coverage increase.

        Flow is same as in _test_generate_example_data2.
        """
        verbose = True
        self._direct_run(verbose)

    def _direct_run(self, verbose: bool) -> None:
        by_date_dir, _ = self._prepare_scratch_space()
        cmd = self._prepare_command_args(by_date_dir, verbose)
        cmd = [c.lstrip("--").split(" ") for c in cmd]
        if verbose:
            # Namespace can be initialized with key value pairs only,
            # so we need to add missing value that is in regular run,
            # always predefined as True if key is specified. Key is --verbose.
            cmd[-1].append("True")
        else:
            cmd.append(["verbose", None])
        kwargs: Dict[str, str] = dict(cmd)
        kwargs.update({"freq": "1H"})
        args = argparse.Namespace(**kwargs)
        generate_pq_example_data._run(args)
        self.check_directory_structure_with_file_contents(by_date_dir)
