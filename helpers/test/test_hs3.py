import os.path
import unittest.mock as umock
from typing import Tuple

import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

# Equivalent to `import moto`, but skip this module if the module is not present.
# `moto` must be imported before `boto3` to properly mock it.
moto = pytest.importorskip("moto")

import helpers.hs3 as hs3  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.hunit_test as hunitest  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position


# TODO(Nikola): Consider moving this class to `hunit_test_case.py`, `hs3.py`, or `hmoto.py`.
class S3Mock_TestCase(hunitest.TestCase):
    # Mocked AWS credentials.
    # TODO(Nikola): Different behaviour if moved in `setUp`?
    mock_aws_credentials_patch = umock.patch.dict(
        hs3.os.environ,
        {
            "AWS_ACCESS_KEY_ID": "mock_key_id",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_access_key",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )
    mock_aws_credentials = None
    # Mocked bucket.
    mock_s3 = moto.mock_s3()
    bucket_name = "mock_bucket"
    moto_client = None

    def setUp(self) -> None:
        super().setUp()
        # It is necessary that boto3 is imported after moto.
        # If not, boto3 will access real AWS.
        import boto3

        # Start boto3 mock.
        self.mock_s3.start()
        # Start aws credentials mock. Must be started after moto mock,
        # or it will be overridden by moto with `foobar` values.
        self.mock_aws_credentials = self.mock_aws_credentials_patch.start()
        # Initialize boto client and create bucket for testing.
        self.moto_client = boto3.client("s3")
        self.moto_client.create_bucket(Bucket=self.bucket_name)

    def tearDown(self) -> None:
        super().tearDown()
        # Stop moto.
        self.mock_s3.stop()
        self.mock_aws_credentials_patch.stop()


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestToFileAndFromFile1(S3Mock_TestCase):
    def write_read_helper(self, file_name: str, force_flush: bool) -> None:
        # Prepare inputs.
        file_content = "line_mock1\nline_mock2\nline_mock3"
        moto_s3fs = hs3.get_s3fs("ck")
        s3_path = f"s3://{self.bucket_name}/{file_name}"
        # Save file.
        # TODO(Nikola): Is it possible to verify `force_flush`?
        hs3.to_file(
            file_content, s3_path, aws_profile=moto_s3fs, force_flush=force_flush
        )
        # Read file.
        saved_content = hs3.from_file(s3_path, aws_profile=moto_s3fs)
        # Check output.
        expected = r"""line_mock1
        line_mock2
        line_mock3"""
        self.assert_equal(saved_content, expected, fuzzy_match=True)

    # #########################################################################

    def test_to_file_and_from_file1(self) -> None:
        """
        Verify that regular `.txt` file is saved/read on S3.
        """
        # Prepare inputs.
        regular_file_name = "mock.txt"
        force_flush = False
        self.write_read_helper(regular_file_name, force_flush)

    def test_to_file_and_from_file2(self) -> None:
        """
        Verify that compressed (e.g,`.gz`,`gzip`) file is saved/read on S3.
        """
        # Prepare inputs.
        gzip_file_name = "mock.gzip"
        force_flush = True
        self.write_read_helper(gzip_file_name, force_flush)

    def test_to_file_invalid1(self) -> None:
        """
        Verify that only binary mode is allowed.
        """
        # Prepare inputs.
        regular_file_name = "mock.txt"
        regular_file_content = "line_mock1\nline_mock2\nline_mock3"
        moto_s3fs = hs3.get_s3fs("ck")
        s3_path = f"s3://{self.bucket_name}/{regular_file_name}"
        # Save file with `t` mode.
        with pytest.raises(ValueError) as fail:
            hs3.to_file(
                regular_file_content, s3_path, mode="wt", aws_profile=moto_s3fs
            )
        # Check output.
        actual = str(fail.value)
        expected = r"S3 only allows binary mode!"
        self.assert_equal(actual, expected)

    def test_from_file_invalid1(self) -> None:
        """
        Verify that encoding is not allowed.
        """
        # Prepare inputs.
        regular_file_name = "mock.txt"
        moto_s3fs = hs3.get_s3fs("ck")
        s3_path = f"s3://{self.bucket_name}/{regular_file_name}"
        # Read with encoding.
        with pytest.raises(ValueError) as fail:
            hs3.from_file(s3_path, encoding=True, aws_profile=moto_s3fs)
        # Check output.
        actual = str(fail.value)
        expected = r"Encoding is not supported when reading from S3!"
        self.assert_equal(actual, expected)


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestListdir1(S3Mock_TestCase):
    def prepare_test_data(self) -> Tuple[str, hs3.AwsProfile]:
        bucket_s3_path = f"s3://{self.bucket_name}"
        depth_one_s3_path = f"{bucket_s3_path}/depth_one"
        # Prepare test files.
        moto_s3fs = hs3.get_s3fs("ck")
        first_s3_path = f"{depth_one_s3_path}/mock1.txt"
        lines = [b"line_mock1"]
        with moto_s3fs.open(first_s3_path, "wb") as s3_file:
            s3_file.writelines(lines)
        second_s3_path = f"{depth_one_s3_path}/mock2.gzip"
        with moto_s3fs.open(second_s3_path, "wb") as s3_file:
            s3_file.writelines(lines)
        # Prepare test directories.
        # `moto_s3fs.mkdir` is useless as empty directory is not visible.
        # There must be at least one file in the directory to be visible.
        regular_dir_s3_path = f"{depth_one_s3_path}/mock"
        additional_file_s3_path = f"{regular_dir_s3_path}/regular_mock3.txt"
        with moto_s3fs.open(additional_file_s3_path, "wb") as s3_file:
            s3_file.writelines(lines)
        git_dir_s3_path = f"s3://{bucket_s3_path}/.git"
        additional_file_s3_path = f"{git_dir_s3_path}/git_mock3.txt"
        with moto_s3fs.open(additional_file_s3_path, "wb") as s3_file:
            s3_file.writelines(lines)
        return bucket_s3_path, moto_s3fs

    # #########################################################################

    def test_listdir1(self) -> None:
        """
        Verify that all paths are found.
        """
        bucket_s3_path, moto_s3fs = self.prepare_test_data()
        pattern = "*"
        only_files = False
        use_relative_paths = False
        paths = hs3.listdir(
            bucket_s3_path,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=moto_s3fs,
            exclude_git_dirs=False,
        )
        paths.sort()
        expected_paths = [
            "mock_bucket/.git",
            "mock_bucket/.git/git_mock3.txt",
            "mock_bucket/depth_one",
            "mock_bucket/depth_one/mock",
            "mock_bucket/depth_one/mock/regular_mock3.txt",
            "mock_bucket/depth_one/mock1.txt",
            "mock_bucket/depth_one/mock2.gzip",
        ]
        self.assertListEqual(paths, expected_paths)

    def test_listdir2(self) -> None:
        """
        Verify that all relative paths are found.
        """
        bucket_s3_path, moto_s3fs = self.prepare_test_data()
        # Exclude `.git` by going level below.
        bucket_s3_path = os.path.join(bucket_s3_path, "depth_one")
        pattern = "*"
        only_files = False
        use_relative_paths = True
        paths = hs3.listdir(
            bucket_s3_path,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=moto_s3fs,
            exclude_git_dirs=False,
        )
        paths.sort()
        expected_paths = [
            "mock",
            "mock/regular_mock3.txt",
            "mock1.txt",
            "mock2.gzip",
        ]
        self.assertListEqual(paths, expected_paths)

    def test_listdir3(self) -> None:
        """
        Verify that all paths are found, except `.git` ones.
        """
        bucket_s3_path, moto_s3fs = self.prepare_test_data()
        pattern = "*"
        only_files = False
        use_relative_paths = False
        paths = hs3.listdir(
            bucket_s3_path,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=moto_s3fs,
        )
        paths.sort()
        expected_paths = [
            "mock_bucket/depth_one",
            "mock_bucket/depth_one/mock",
            "mock_bucket/depth_one/mock/regular_mock3.txt",
            "mock_bucket/depth_one/mock1.txt",
            "mock_bucket/depth_one/mock2.gzip",
        ]
        self.assertListEqual(paths, expected_paths)

    def test_listdir4(self) -> None:
        """
        Verify that all file paths are found.
        """
        bucket_s3_path, moto_s3fs = self.prepare_test_data()
        pattern = "*"
        only_files = True
        use_relative_paths = False
        paths = hs3.listdir(
            bucket_s3_path,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=moto_s3fs,
            exclude_git_dirs=False,
        )
        paths.sort()
        expected_paths = [
            "mock_bucket/.git/git_mock3.txt",
            "mock_bucket/depth_one/mock/regular_mock3.txt",
            "mock_bucket/depth_one/mock1.txt",
            "mock_bucket/depth_one/mock2.gzip",
        ]
        self.assertListEqual(paths, expected_paths)


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestDu1(S3Mock_TestCase):
    def test_du1(self) -> None:
        """
        Verify that total file size is returned.
        """
        bucket_s3_path = f"s3://{self.bucket_name}"
        depth_one_s3_path = f"{bucket_s3_path}/depth_one"
        # Prepare test files.
        moto_s3fs = hs3.get_s3fs("ck")
        first_s3_path = f"{bucket_s3_path}/mock1.txt"
        lines = [b"line_mock\n"] * 150
        with moto_s3fs.open(first_s3_path, "wb") as s3_file:
            s3_file.writelines(lines)
        second_s3_path = f"{depth_one_s3_path}/mock2.txt"
        with moto_s3fs.open(second_s3_path, "wb") as s3_file:
            # One level deeper to test recursive `du`.
            s3_file.writelines(lines)
        # Get multiple files.
        size = hs3.du(bucket_s3_path, aws_profile=moto_s3fs)
        expected_size = 3000
        self.assertEqual(size, expected_size)
        size = hs3.du(depth_one_s3_path, aws_profile=moto_s3fs)
        expected_size = 1500
        self.assertEqual(size, expected_size)
        # Get exactly one file.
        size = hs3.du(second_s3_path, aws_profile=moto_s3fs)
        self.assertEqual(size, expected_size)
        # Verify size in human-readable form.
        size = hs3.du(bucket_s3_path, human_format=True, aws_profile=moto_s3fs)
        expected_size = r"2.9 KB"
        self.assert_equal(size, expected_size)
