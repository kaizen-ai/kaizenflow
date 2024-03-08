import os

import pytest
from moto import mock_s3

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest


class Test_get_service_client(hunitest.TestCase):
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
        Test `haws.get_service_client()` returns a client for s3.
        """
        aws_profile = "__mock__"
        service_name = "s3"
        region = "us-east-1"
        # Create mock client for s3.
        client = haws.get_service_client(
            aws_profile=aws_profile, service_name=service_name, region=region
        )
        # Check that the returned client is for the S3 service.
        hdbg.dassert_eq(client.meta.service_model.service_name, "s3")
        # Check for region.
        hdbg.dassert_eq(client.meta.region_name, region)
