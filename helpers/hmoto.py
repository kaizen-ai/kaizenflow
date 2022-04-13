"""
Import as:

import helpers.hmoto as hmoto
"""

import abc
import unittest.mock as umock

import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

# Equivalent to `import moto`, but skip this module if the module is not present.
# `moto` must be imported before `boto3` to properly mock it.
moto = pytest.importorskip("moto")

import helpers.hs3 as hs3  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-positiona
import helpers.hunit_test as hunitest  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import im_v2.common.db.db_utils as imvcddbut  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-positiona


class S3Mock_Base(abc.ABC):
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
        # Precaution to ensure that we are using mocked botocore.
        test_client = boto3.client("s3")
        buckets = test_client.list_buckets()["Buckets"]
        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets[0]["Name"], self.bucket_name)

    def tearDown(self) -> None:
        super().tearDown()
        # Stop moto.
        self.mock_s3.stop()
        self.mock_aws_credentials_patch.stop()


class S3Mock_TestCase(S3Mock_Base, hunitest.TestCase):
    pass


class S3Mock_TestImDbHelper(S3Mock_Base, imvcddbut.TestImDbHelper):
    pass
