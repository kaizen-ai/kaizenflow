import os

import helpers.hgit as hgit
import helpers.hunit_test as hunitest
import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut


class TestGetDataSnapshot(hunitest.TestCase):
    @staticmethod
    def get_base_test_dir() -> str:
        base_dir = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data_snapshot/test/test_data_snapshots",
        )
        return base_dir

    def test_get_data_snapshot1(self) -> None:
        """
        All data snapshots in `root_dir` are numeric.
        """
        base_dir = self.get_base_test_dir()
        test_dir = os.path.join(base_dir, "numeric_data_snapshots")
        aws_profile = None
        data_snapshot = "latest"
        latest_data_snapshot = imvcdsdsut.get_data_snapshot(
            test_dir, data_snapshot, aws_profile
        )
        expected = "20220720"
        self.assert_equal(latest_data_snapshot, expected)

    def test_get_data_snapshot2(self) -> None:
        """
        `root_dir` contains alpha-numeric data snapshots.
        """
        base_dir = self.get_base_test_dir()
        test_dir = os.path.join(base_dir, "alpha_numeric_data_snapshots")
        aws_profile = None
        data_snapshot = "latest"
        latest_data_snapshot = imvcdsdsut.get_data_snapshot(
            test_dir, data_snapshot, aws_profile
        )
        expected = "20220130"
        self.assert_equal(latest_data_snapshot, expected)

    def test_get_data_snapshot3(self) -> None:
        """
        `root_dir` doesn't contain numeric data snapshot.
        """
        test_dir = self.get_base_test_dir()
        aws_profile = None
        data_snapshot = "latest"
        with self.assertRaises(AssertionError):
            imvcdsdsut.get_data_snapshot(test_dir, data_snapshot, aws_profile)
