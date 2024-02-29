import logging
import os

import boto3
import pytest
from moto import mock_s3

import helpers.haws as haws
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class test_get_service_resource(hunitest.TestCase):
    @pytest.fixture(autouse=True, scope="class")
    def aws_credentials(self):
        """
        Mocked AWS Credentials for moto.
        """
        os.environ["MOCK_AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["MOCK_AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["MOCK_AWS_SECURITY_TOKEN"] = "testing"
        os.environ["MOCK_AWS_SESSION_TOKEN"] = "testing"
        os.environ["MOCK_AWS_DEFAULT_REGION"] = "us-east-1"

    @mock_s3
    def test_s3_resource(self):
        """
        Test that get_service_resource correctly retrieves an S3 resource.
        """
        aws_profile = "__mock__"
        service_name = "s3"

        # Creating a mock s3 bucket.
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket="my-test-bucket")

        s3_resource = haws.get_service_resource(aws_profile, service_name)

        buckets = list(s3_resource.buckets.all())
        bucket_names = [bucket.name for bucket in buckets]

        # Assert
        self.assertIn("my-test-bucket", bucket_names)
