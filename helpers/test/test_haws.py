import logging
import os

import boto3
import pytest
from moto import mock_ecs

import helpers.haws as haws
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_update_task_definition(hunitest.TestCase):
    @pytest.fixture(autouse=True, scope="class")
    def aws_credentials(self) -> None:
        """
        Mocked AWS Credentials for moto.
        """
        os.environ["MOCK_AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["MOCK_AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["MOCK_AWS_SECURITY_TOKEN"] = "testing"
        os.environ["MOCK_AWS_SESSION_TOKEN"] = "testing"
        os.environ["MOCK_AWS_DEFAULT_REGION"] = "us-east-1"

    @mock_ecs
    def test1(self) -> None:
        # Mock data
        task_definition_name = "my-task-definition"
        old_image_url = "old_image_url"
        new_image_url = "new_image_url"
        region = "us-east-1"

        # Create ECS client using boto3
        ecs_client = boto3.client("ecs", region_name=region)

        # Create a mock task definition
        ecs_client.register_task_definition(
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

        # Update task definition
        haws.update_task_definition(
            task_definition_name, new_image_url, region=region
        )

        # Check if the task definition is updated
        task_description = ecs_client.describe_task_definition(
            taskDefinition=task_definition_name
        )
        updated_image_url = task_description["taskDefinition"][
            "containerDefinitions"
        ][0]["image"]
        self.assertEqual(updated_image_url, new_image_url)