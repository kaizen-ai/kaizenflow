import os
import unittest.mock as umock

import boto3
import pytest
from botocore.client import BaseClient
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


class Test_update_task_definition(hunitest.TestCase):
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
    @umock.patch("helpers.haws.get_ecs_client")
    def test1(self, mock_get_ecs_client: BaseClient) -> None:
        """
        Test updating a task definition with a new image URL.
        """
        # Mock data.
        task_definition_name = "my-task-definition"
        old_image_url = "old_image_url"
        new_image_url = "new_image_url"
        region = "us-east-1"
        # Mock the return value of `get_ecs_client`.
        mock_client = boto3.client("ecs", region_name=region)
        mock_get_ecs_client.return_value = mock_client
        # Create a mock task definition.
        mock_client.register_task_definition(
            family=task_definition_name,
            containerDefinitions=[
                {"name": "my-container", "image": old_image_url}
            ],
            executionRoleArn="__mock__",
            networkMode="bridge",
            requiresCompatibilities=["EC2"],
            cpu="256",
            memory="512",
        )
        # Update task definition.
        haws.update_task_definition(
            task_definition_name, new_image_url, region=region
        )
        # Check if the task definition is updated.
        task_description = mock_client.describe_task_definition(
            taskDefinition=task_definition_name
        )
        updated_image_url = task_description["taskDefinition"][
            "containerDefinitions"
        ][0]["image"]
        self.assertEqual(updated_image_url, new_image_url)