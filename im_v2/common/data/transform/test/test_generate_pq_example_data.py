import os

import pytest

import helpers.git as hgit
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest


class TestGeneratePqExampleData1(hunitest.TestCase):
    def generate_test_data(self, verbose: bool) -> str:
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

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def _test_generate_example_data1(self) -> None:
        """
        Generate daily data for 3 days in basic by-date format.
        """
        verbose = False
        by_date_dir = self.generate_test_data(verbose)
        self.check_directory_structure_with_file_contents(by_date_dir)

    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def _test_generate_example_data2(self) -> None:
        """
        Generate daily data for 3 days in verbose by-date format.
        """
        verbose = True
        by_date_dir = self.generate_test_data(verbose)
        self.check_directory_structure_with_file_contents(by_date_dir)
