import os

import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest


class TestGeneratePqExampleData1(hunitest.TestCase):
    def test_example_data1(self) -> None:
        """
        Generate daily data for 3 days in a by-date format.
        """
        test_dir = self.get_scratch_space()
        # Generate some data.
        cmd = []
        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/test/generate_pq_example_data.py",
        )
        cmd.append(file_path)
        cmd.append("--start_date 2021-12-30")
        cmd.append("--end_date 2022-01-02")
        cmd.append("--assets A,B,C")
        cmd.append(f"--dst_dir {test_dir}")
        cmd = " ".join(cmd)
        hsystem.system(cmd)
        # Check directory structure with file contents.
        include_file_content = True
        by_date_signature = hunitest.get_dir_signature(
            test_dir, include_file_content
        )
        actual = []
        actual.append("# test_data=")
        actual.append(by_date_signature)
        actual = "\n".join(actual)
        self.check_string(actual, purify_text=True)
