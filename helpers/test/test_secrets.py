import logging
import os

import helpers.secrets as hsecret
import helpers.unit_test as hunitest
import pytest
import boto3
import json
from botocore.client import BaseClient
from moto import mock_secretsmanager

_LOG = logging.getLogger(__name__)

class Test_Create_Client(hunitest.TestCase):
    """
    Simple smoke test to verify cconnection to AWS
    """
    def test_create_client1(self) -> None:
        client = hsecret.get_secrets_client()
        self.assertIsInstance(client, BaseClient)
    raise NotImplementedError

@mock_secretsmanager
class Test_Get_Secret(hunitest.TestCase):
    """
    Verify that the secret can be retrieved correctly
    """
    # the client is mocked so region name can be anything
    def test_create_client1(self) -> None:
        client = boto3.client("secretsmanager", region_name="eu-north-1")
        secret = {"testkey": "testvalue"}
        secret_name = "Testsecret"
        client.create_secret(Name=secret_name, SecretString=json.dumps(secret))
        self.assert_equal(hsecret.get_secret(secret_name), secret)

@mock_secretsmanager
class Test_Store_Secret(hunitest.TestCase):
    """
    Verify that a secret can be stored correctly
    """
    raise NotImplementedError