_HAS_MOTO = True
try:
    import moto
except ImportError:
    # `moto` is not installed in `dev_tools`, so we skip it (see "DevTools376:
    # Break 2022-02-22").
    import helpers.hgit as hgit

    assert (
        hgit.is_amp() or hgit.is_dev_tools()
    ), "Only `amp` or `dev_tools` can skip these tests."
    _HAS_MOTO = False

if _HAS_MOTO:
    import json
    import logging

    import boto3
    import botocore
    import pytest

    import helpers.hsecrets as hsecret
    import helpers.hunit_test as hunitest

    _LOG = logging.getLogger(__name__)

    # The `mock_secretsmanager` decorator ensures the calls to the AWS API are
    # mocked.

    _REASON_TO_SKIP_TEST = "Need to add `ck` profile to GH actions CmTask961, test has passed on dev stage."

    class TestCreateClient(hunitest.TestCase):
        @pytest.mark.skip(reason=_REASON_TO_SKIP_TEST)
        def test_create_client1(self) -> None:
            """
            Simple smoke test to verify connection to AWS.
            """
            client = hsecret.get_secrets_client()
            self.assertIsInstance(client, botocore.client.BaseClient)

    class TestGetSecret(hunitest.TestCase):
        @pytest.mark.skip(reason=_REASON_TO_SKIP_TEST)
        @moto.mock_secretsmanager
        def test_get_secret(self) -> None:
            """
            Verify that the secret can be retrieved correctly.
            """
            # Make sure the region name matches the one used in `hsecret` profile.
            client = boto3.client("secretsmanager", region_name="eu-north-1")
            secret = {"testkey": "testvalue"}
            secret_name = "Testsecret"
            client.create_secret(
                Name=secret_name, SecretString=json.dumps(secret)
            )
            self.assertDictEqual(hsecret.get_secret(secret_name), secret)

    class TestStoreSecret(hunitest.TestCase):
        @pytest.mark.skip(reason=_REASON_TO_SKIP_TEST)
        @moto.mock_secretsmanager
        def test_store_secret1(self) -> None:
            """
            Verify that a secret can be stored correctly.
            """
            secret = {"testkey": "testvalue"}
            secret_name = "Testsecret"
            hsecret.store_secret(secret_name, secret)
            # Make sure the region name matches the one used in `hsecret`.
            client = boto3.client("secretsmanager", region_name="eu-north-1")
            test_secret_value = json.loads(
                client.get_secret_value(SecretId=secret_name)["SecretString"]
            )
            self.assertDictEqual(test_secret_value, secret)
