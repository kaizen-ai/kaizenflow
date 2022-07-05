import helpers.hunit_test as hunitest
import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut


class TestGetLatestDataSnapshot(hunitest.TestCase):
    def test_get_latest_data_snapshot1(self) -> None:
        """
        All data snapshots in `root_dir` are numeric.
        """
        root_dir = "im_v2/common/data_snapshot/test/test_data_snapshots/numeric_data_snapshots"
        aws_profile = None
        latest_data_snapshot = imvcdsdsut.get_latest_data_snapshot(
            root_dir, aws_profile
        )
        expected = "20220720"
        self.assert_equal(latest_data_snapshot, expected)

    def test_get_latest_data_snapshot2(self) -> None:
        """
       `root_dir` contains `latest` data snapshot.
        """
        root_dir = "im_v2/common/data_snapshot/test/test_data_snapshots/alpha_numeric_data_snapshots"
        aws_profile = None
        latest_data_snapshot = imvcdsdsut.get_latest_data_snapshot(
            root_dir, aws_profile
        )
        expected = "20220130"
        self.assert_equal(latest_data_snapshot, expected)

    def test_get_latest_data_snapshot3(self) -> None:
        """
        `root_dir` doesn't contain numeric data snapshot.
        """
        root_dir = "im_v2/common/data_snapshot/test/test_data_snapshots"
        aws_profile = None
        with self.assertRaises(AssertionError):
            imvcdsdsut.get_latest_data_snapshot(
                root_dir, aws_profile
            )