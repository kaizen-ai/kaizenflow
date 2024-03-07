import os
import boto3
import pytest
from moto import mock_ecs
import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest


class Test_get_ecs_client(hunitest.TestCase):
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
    def test1(self) -> None:
        """
        Test that `haws.get_ecs_client()` correctly return a client to work with ECS.
        """
        aws_profile = "__mock__"
        test_cluster_name = 'TEST-CLUSTER'
        region = "us-east-1"
        
        # Create mock ecs client
        ecs_client = boto3.client('ecs', region_name=region)
        _ = ecs_client.create_cluster(clusterName=test_cluster_name)

        test_client = haws.get_ecs_client(aws_profile, region=region)

        # Get the created cluster
        cluster_name = test_client.list_clusters()['clusterArns'][0]
        print(cluster_name)

        # Check if the region and cluster name in the created cluster
        hdbg.dassert_in(test_cluster_name, cluster_name)
        hdbg.dassert_in(region, cluster_name)
