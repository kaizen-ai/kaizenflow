import os
import unittest.mock as umock

import boto3
import pytest
from moto import mock_ecs, mock_s3

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


class Test_get_task_definition_image_url(hunitest.TestCase):
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

    @mock_ecs
    @umock.patch("helpers.haws.get_service_client")
    def test1(self, mock_get_service_client: umock.Mock) -> None:
        """
        Test that `get_task_definition_image_url` retrieves correct image URL.
        """
        # Mock data.
        task_definition_name = "my-task-definition"
        mock_image_url = "old_image_url"
        region = "us-east-1"
        # Mock the return value of `get_service_client`.
        mock_client = boto3.client("ecs", region_name=region)
        mock_get_service_client.return_value = mock_client
        # Create a mock task definition.
        mock_client.register_task_definition(
            family=task_definition_name,
            containerDefinitions=[
                {"name": "my-container", "image": mock_image_url}
            ],
        )
        image_url = haws.get_task_definition_image_url(task_definition_name)
        self.assertEqual(image_url, mock_image_url)
