# TODO(gp): Use pytest.import_skip instead of all this machinery.
_HAS_MOTO = True
try:
    import moto
except ImportError:
    # `moto` is not installed in `dev_tools`, so we skip it (see "DevTools376:
    # Break 2022-02-22").
    import helpers.hgit as hgit

    assert not hgit.is_cmamp(), (
        "`cmamp` should have moto, while other repos (e.g., `amp` or `dev_tools`) "
        "are allowed to not have it)"
    )
    _HAS_MOTO = False

if _HAS_MOTO:
    import json
    import logging
    import unittest.mock as umock

    import boto3
    import botocore
    import pytest

    import helpers.henv as henv
    import helpers.hgit as hgit
    import helpers.hsecrets as hsecret
    import helpers.hunit_test as hunitest

    _LOG = logging.getLogger(__name__)

    # The `mock_secretsmanager` decorator ensures the calls to the AWS API are
    # mocked.

    @pytest.mark.requires_ck_infra
    @pytest.mark.requires_aws
    @pytest.mark.skipif(
        not henv.execute_repo_config_code("is_CK_S3_available()"),
        reason="Run only if CK S3 is available",
    )
    class TestCreateClient(hunitest.TestCase):
        def test_create_client1(self) -> None:
            """
            Simple smoke test to verify connection to AWS.
            """
            client = hsecret.get_secrets_client(aws_profile="ck")
            self.assertIsInstance(client, botocore.client.BaseClient)

    @pytest.mark.requires_ck_infra
    @pytest.mark.requires_aws
    @pytest.mark.skipif(
        not henv.execute_repo_config_code("is_CK_S3_available()"),
        reason="Run only if CK S3 is available",
    )
    class TestGetSecret(hunitest.TestCase):
        @moto.mock_secretsmanager
        def test_get_secret(self) -> None:
            """
            Verify that the secret can be retrieved correctly.
            """
            # Make sure the region name matches the one used in `hsecret` profile.
            client = boto3.client("secretsmanager", region_name="eu-north-1")
            secret = {"testkey": "testvalue"}
            secret_name = "test.local.sandbox.1"
            client.create_secret(
                Name=secret_name, SecretString=json.dumps(secret)
            )
            self.assertDictEqual(hsecret.get_secret(secret_name), secret)

        @moto.mock_secretsmanager
        def test_trading_key(self) -> None:
            """
            Verify locking mechanism for trading key is processed correctly.
            """
            # Define test params.
            secret_value = {"test.trading.key": "test.trading.value"}
            secret_name = "test.trading.sandbox.1"
            usedBy = "pytest"
            hsecret.store_secret(secret_name, secret_value)
            # Define expected values.
            usedBy = hsecret._get_flag_value(usedBy)
            expected = f"Secret key is already in use by {usedBy}"
            # Call get secret to lock the key.
            _ = hsecret.get_secret(secret_name)
            # Recall get secret for same key to verify the lock.
            try:
                hsecret.get_secret(secret_name)
            except RuntimeError as rte:
                actual = str(rte)
                self.assert_equal(actual, expected, fuzzy_match=True)

        @moto.mock_secretsmanager
        def test_lock_for_different_script(self) -> None:
            """
            Verify locking mechanism for access to trading key is passed if
            scripts are different.
            """
            # Define test params.
            secret_value = {"test.trading.key": "test.trading.value"}
            secret_name = "test.trading.sandbox.1"
            script1 = "pytest"
            script2 = "run_system_observer.py"
            hsecret.store_secret(secret_name, secret_value)
            # Call get secret to lock the key with testing script.
            _ = hsecret.get_secret(secret_name)
            usedBy1 = hsecret._get_flag_value(script1)
            # Define expected values.
            usedBy2 = hsecret._get_flag_value(script2)
            # Update secret value with expected usedBy script names.
            secret_value["usedBy"] = [usedBy1, usedBy2]
            # Call get secret for same key to verify the lock for mocked script.
            with umock.patch("sys.argv", [script2]):
                actual = hsecret.get_secret(secret_name)
            self.assert_equal(
                str(actual), expected=str(secret_value), fuzzy_match=True
            )

    @pytest.mark.requires_ck_infra
    @pytest.mark.requires_aws
    @pytest.mark.skipif(
        not henv.execute_repo_config_code("is_CK_S3_available()"),
        reason="Run only if CK S3 is available",
    )
    class TestStoreSecret(hunitest.TestCase):
        @moto.mock_secretsmanager
        def test_store_secret1(self) -> None:
            """
            Verify that a secret can be stored correctly.
            """
            secret = {"testkey": "testvalue"}
            secret_name = "test.local.sandbox.1"
            hsecret.store_secret(secret_name, secret)
            # Make sure the region name matches the one used in `hsecret`.
            client = boto3.client("secretsmanager", region_name="eu-north-1")
            test_secret_value = json.loads(
                client.get_secret_value(SecretId=secret_name)["SecretString"]
            )
            self.assertDictEqual(test_secret_value, secret)

    @pytest.mark.requires_ck_infra
    @pytest.mark.requires_aws
    @pytest.mark.skipif(
        not henv.execute_repo_config_code("is_CK_S3_available()"),
        reason="Run only if CK S3 is available",
    )
    class TestLockSecret(hunitest.TestCase):
        @moto.mock_secretsmanager
        def test_lock_secret(self) -> None:
            """
            Verify that the lock secret function locks the key.
            """
            # Define test params.
            secret = {"testkey": "testvalue"}
            secret_name = "test.local.sandbox.1"
            hsecret.store_secret(secret_name, secret)
            usedBy = "pytest"
            # Lock the stored secret.
            hsecret.lock_secret(secret_name, secret)
            # Retry locking the same secret.
            try:
                hsecret.lock_secret(secret_name, secret)
            except RuntimeError as rte:
                usedBy = hsecret._get_flag_value(usedBy)
                expected = f"Secret key is already in use by {usedBy}"
                actual = str(rte)
                self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.requires_ck_infra
    @pytest.mark.requires_aws
    @pytest.mark.skipif(
        not henv.execute_repo_config_code("is_CK_S3_available()"),
        reason="Run only if CK S3 is available",
    )
    class TestUpdateUsedby(hunitest.TestCase):
        @moto.mock_secretsmanager
        def test1(self) -> None:
            """
            Verify that update_usedby updates value in secrets manager.
            """
            # Define test params.
            secret_value = {"testkey": "testvalue"}
            secret_name = "test.local.sandbox.1"
            usedBy = "pytest"
            hsecret.store_secret(secret_name, secret_value)
            # Define expected value.
            expected = r"""
            {'testkey': 'testvalue', 'usedBy': ['pytest']}
            """
            # Run.
            hsecret.update_usedby(secret_name, secret_value, usedBy)
            actual = hsecret.get_secret(secret_name)
            # Verify.
            self.assert_equal(str(actual), expected, fuzzy_match=True)
