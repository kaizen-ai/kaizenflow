from moto import mock_ecs
import boto3
import logging
import pytest
import os
import tempfile

from unittest.mock import patch
import helpers.haws as haws
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

class Test_get_task_definition_image_url(hunitest.TestCase):
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
    @patch('helpers.haws.get_service_client')
    def test_get_task_definition_image_url(self, mock_get_service_client):
        """
        Test that get_task_definition_image_url retrieves correct image URL.
        """
        # Mock data
        task_definition_name = "my-task-definition"
        old_image_url = "old_image_url"
        region = "us-east-1"

        # Mock the return value of get_service_client
        mock_client = boto3.client("ecs", region_name=region)
        mock_get_service_client.return_value = mock_client

        # Create a mock task definition~
        mock_client.register_task_definition(
            family=task_definition_name,
            containerDefinitions=[
                {
                    "name": "my-container", 
                    "image": old_image_url
                }
            ],
        )

        # Calling the function
        image_url = haws.get_task_definition_image_url(task_definition_name)

        # Assert
        assert image_url == old_image_url


    