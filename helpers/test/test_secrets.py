import json
import logging

import boto3
from botocore.client import BaseClient
from moto import mock_secretsmanager

import helpers.secrets as hsecret
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

# mock_secretsmanager decorator ensures the calls to the AWS API are mocked


class Test_Create_Client(hunitest.TestCase):
    """
    Simple smoke test to verify cconnection to AWS.
    """

    def test_create_client1(self) -> None:
        client = hsecret.get_secrets_client()
        self.assertIsInstance(client, BaseClient)


@mock_secretsmanager
class Test_Get_Secret(hunitest.TestCase):
    """
    Verify that the secret can be retrieved correctly.
    """

    @mock_secretsmanager
    def test_get_secret(self) -> None:
        # make sure the region name matches the one used in hsecret
        client = boto3.client("secretsmanager", region_name="eu-north-1")
        secret = {"testkey": "testvalue"}
        secret_name = "Testsecret"

        client.create_secret(Name=secret_name, SecretString=json.dumps(secret))
        self.assertDictEqual(hsecret.get_secret(secret_name), secret)


class Test_Store_Secret(hunitest.TestCase):
    """
    Verify that a secret can be stored correctly.
    """

    @mock_secretsmanager
    def test_store_secret(self) -> None:
        secret = {"testkey": "testvalue"}
        secret_name = "Testsecret"
        hsecret.store_secret(secret_name, secret)

        # make sure the region name matches the one used in hsecret
        client = boto3.client("secretsmanager", region_name="eu-north-1")
        test_secret_value = json.loads(
            client.get_secret_value(SecretId=secret_name)["SecretString"]
        )
        self.assertDictEqual(test_secret_value, secret)
