import unittest.mock as umock
import pytest

try:
    import moto

    _HAS_MOTO = True
except ImportError:
    _HAS_MOTO = False

import helpers.hs3 as hs3
import helpers.hunit_test as hunitest

if _HAS_MOTO:

    class S3Mock(hunitest.TestCase):
        # Mocked AWS credentials.
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
            # It is very important that boto3 is imported after moto.
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
            # Stop boto3 mock.
            self.mock_s3.stop()
            # Stop mock aws credentials.
            self.mock_aws_credentials_patch.stop()

    @pytest.mark.skip("Enable after CMTask1292 is resolved.")
    class TestToFile1(S3Mock):

        def test_to_file1(self) -> None:
            """
            Verify that regular `.txt` file is saved on S3.
            """
            # Prepare inputs.
            regular_file_name = "mock.txt"
            regular_file_content = "line_mock1\nline_mock2\nline_mock3"
            moto_s3fs = hs3.get_s3fs("ck")
            s3_path = f"s3://{self.bucket_name}/{regular_file_name}"
            # Save file.
            hs3.to_file(regular_file_content, s3_path, aws_profile=moto_s3fs)
            # Check output.
            saved_files = moto_s3fs.ls(self.bucket_name)
            self.assertListEqual(saved_files, [s3_path.lstrip("s3://")])
            with moto_s3fs.open(s3_path.lstrip("s3://")) as saved_file:
                saved_content = saved_file.read().decode()
            expected = r"""line_mock1
            line_mock2
            line_mock3"""
            self.assert_equal(saved_content, expected, fuzzy_match=True)