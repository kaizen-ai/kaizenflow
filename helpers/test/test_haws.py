import os
from typing import Optional

import boto3
import pytest
from moto import mock_s3

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest

class Test_get_session(hunitest.TestCase):
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self):
        # Run before each test.
        self.set_up_test()
        yield
        # Run after each test.
        self.tear_down_test()

    def set_up_test(self) -> None:
        self.setUp()
        os.environ["MOCK_AWS_ACCESS_KEY_ID"] = "mock_access_key"
        os.environ["MOCK_AWS_SECRET_ACCESS_KEY"] = "mock_secret_access_key"
        os.environ["MOCK_AWS_S3_BUCKET"] = "mock_s3_bucket"
        os.environ["MOCK_AWS_DEFAULT_REGION"] = "us-east-1"

    def tear_down_test(self) -> None:
        del os.environ["MOCK_AWS_ACCESS_KEY_ID"]
        del os.environ["MOCK_AWS_SECRET_ACCESS_KEY"]
        del os.environ["MOCK_AWS_S3_BUCKET"]
        del os.environ["MOCK_AWS_DEFAULT_REGION"]

    def mock_session(self, region: Optional[str] = None) -> None:
        aws_profile = "__mock__"
        # Create mock session.
        mock_session = boto3.session.Session(
            aws_access_key_id="mock_access_key",
            aws_secret_access_key="mock_secret_access_key",
            region_name="us-east-1",
        )
        # Using mock session to create a S3 bucket.
        s3_resource = mock_session.resource("s3")
        s3_resource.create_bucket(Bucket="my-bucket")
        if region:
            session = haws.get_session(aws_profile, region = region)
        else:
            session = haws.get_session(aws_profile)
        # Get all S3 buckets in session.
        s3_client = session.client("s3")
        response = s3_client.list_buckets()
        bucket_names = [bucket["Name"] for bucket in response.get("Buckets", [])]
        # Check if they are matched.
        self.assertIn("my-bucket", bucket_names)
    
    @mock_s3
    def test_get_session1(self) -> None:
        """
        Test that `haws.get_session` correctly return a session without region
        parameter.
        """
        self.mock_session()

    @mock_s3
    def test_get_session2(self) -> None:
        """
        Test that `haws.get_session` correctly return a session with region
        parameter.
        """
        self.mock_session(region="us-east-1")

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
