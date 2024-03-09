import os

import boto3
import pytest
from moto import mock_s3

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest


class Test_get_service_resource(hunitest.TestCase):
    @pytest.fixture(autouse=True, scope="class")
    def aws_credentials(self) -> None:
        """
        Mocked AWS credentials for moto.
        """
        os.environ["MOCK_AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["MOCK_AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["MOCK_AWS_SECURITY_TOKEN"] = "testing"
        os.environ["MOCK_AWS_SESSION_TOKEN"] = "testing"
        os.environ["MOCK_AWS_DEFAULT_REGION"] = "us-east-1"

    @mock_s3
    def test1(self) -> None:
        """
        Test that `haws.get_service_resource()` correctly retrieves a s3
        resource.
        """
        aws_profile = "__mock__"
        service_name = "s3"
        # Create mock s3 bucket.
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket="my-test-bucket")
        s3_resource = haws.get_service_resource(aws_profile, service_name)
        # Get all `s3` buckets.
        buckets = list(s3_resource.buckets.all())
        bucket_names = [bucket.name for bucket in buckets]
        # Check.
        hdbg.dassert_in("my-test-bucket", bucket_names)
