import unittest.mock as umock

# Equivalent to `import moto`, but skip this module if the module is not present.
import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

# `moto` must be imported before `boto3` to properly mock it.
moto = pytest.importorskip("moto")

import helpers.hs3 as hs3  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.hunit_test as hunitest  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position


# TODO(Nikola): Consider moving this class to `hunit_test_case.py`, `hs3.py`, or `hmoto.py`.
class S3TestCase(hunitest.TestCase):
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
class TestToFileAndFromFile1(S3TestCase):

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