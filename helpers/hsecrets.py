"""
Import as:

import helpers.hsecrets as hsecret
"""

import atexit
import json
import sys
import warnings
from typing import Any, Dict, Optional

from botocore.client import BaseClient
from botocore.exceptions import ClientError

import helpers.haws as haws
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg


def get_secrets_client(aws_profile: str) -> BaseClient:
    """
    Return client to work with AWS Secrets Manager in the specified region.
    """
    session = haws.get_session(aws_profile)
    client = session.client(service_name="secretsmanager")
    return client


def _get_flag_value(flag: str) -> str:
    """
    Return flag value with concatenated date string.

    E.g., for flag = 'pytest' return 'pytest_20240619'.
    """
    timestamp = hdateti.get_current_date_as_string("naive_ET")
    updated_flag = "_".join([flag, timestamp])
    return updated_flag


def lock_secret(
    secret_name: str, secret_value: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Lock access to a secret to the current script.

    Lock access to secret key with trading keyword in `secret_name`, for a
    runtime instance of a script, to avoid parallel run.
    Add the script name to `usedBy` list in the AWS secret manager.
    Raise error if the same script tries to access a locked key.

    :param secret_name: SecretId of record to be updated.
    :param secret_value: Current value of SecretString.
    :return secret_value: SecretString with updated `usedBy` script if not
        already locked.
    """
    current_script = sys.argv[0].split("/")[-1]
    # Check if the current script is already using this secret.
    current_usedBy = list(
        filter(lambda x: current_script in x, secret_value.get("usedBy", []))
    )
    # Check current value of usedBy to determine further action.
    if not current_usedBy:
        # Fetch and update value of usedBy if not locked.
        usedBy = _get_flag_value(current_script)
        secret_value = update_usedby(secret_name, secret_value, usedBy)
        # Release secret key lock on termination.
        atexit.register(
            update_usedby, secret_name, secret_value, usedBy, remove=True
        )
    else:
        # Raise warning of locked resource with current use info.
        # raise RuntimeError()
        warnings.warn(
            f"Secret key is already in use by {current_usedBy[0]}", RuntimeWarning
        )
    return secret_value


def update_usedby(
    secret_name: str,
    secret_value: Dict[str, Any],
    usedBy: str,
    *,
    remove: bool = False,
) -> Dict[str, Any]:
    """
    Update the value of `usedBy` attribute from `secret_value` in AWS secrets
    manager to lock the key. Unlock the key at the end of process using default
    value of `usedBy`.

    :param secret_name: SecretId of record to be updated.
    :param secret_value: Current value of SecretString.
    :param usedBy: value of `usedBy` to be updated. Used to remove from
        list on deallocation of resource, i.e., when remove is True.
    :param remove: Boolean to decide addition or removal of `usedBy` value
        in the secret value list of scripts. Default is False.
    :return secret_value: SecretString with updated `usedBy` script.
    """
    hdbg.dassert_isinstance(secret_name, str)
    aws_profile = "ck"
    client = get_secrets_client(aws_profile)
    # Modify value of used by in secret value.
    if not remove:
        try:
            secret_value["usedBy"].append(usedBy)
        except KeyError:
            secret_value["usedBy"] = [usedBy]
    else:
        secret_value["usedBy"].remove(usedBy)
    # Update the modified secret value in AWS secret manager.
    client.update_secret(
        SecretId=secret_name, SecretString=json.dumps(secret_value)
    )
    return secret_value


# TODO(Juraj): add support to access secrets for different profiles, not important rn
def get_secret(secret_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch secret values(s) from AWS secrets manager.

    :return a dictionary of key-value pairs. E.g., `get_secret('binance')` returns
    ```
    {
        'apiKey': '<secret_value>',
        'secret': '<secret_value>'
    }
    ```
    """
    # TODO(Juraj): This assertion can't be applied universally.
    # Check if the secret name format is valid.
    # dassert_valid_secret(secret_name)
    hdbg.dassert_isinstance(secret_name, str)
    # Create a AWS Secrets Manager client.
    aws_profile = "ck"
    client = get_secrets_client(aws_profile)
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # for the full list of exceptions.
    # Define access key to check the entity requesting for secret key.
    access_key = "trading"
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response["SecretString"]
        hdbg.dassert_isinstance(secret_string, str)
        secret_val = json.loads(secret_string)
        # Check access entity value to lock secret key to avoid parallel run.
        if access_key in secret_name:
            secret_val = lock_secret(secret_name, secret_val)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # Let user know the secret does not exist.
            raise ValueError("No such secret: %s" % secret_name) from e
        # If not yet implemented handler then just re-raise.
        raise e
    return secret_val


# TODO(Juraj): add support to store secrets in different regions, not important rn.
def store_secret(
    secret_name: str, secret_value: Dict[str, str], *, description: str = ""
) -> Optional[bool]:
    """
    Store secret values(s) into AWS secrets manager, specify secret as a dict
    of key-value pairs.

    :return: bool representing whether writing was successful or not
    """
    hdbg.dassert_isinstance(secret_name, str)
    # Create a AWS Secrets Manager client.
    aws_profile = "ck"
    client = get_secrets_client(aws_profile)
    # See
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_CreateSecret.html
    # for the full list of exceptions.
    try:
        create_secret_value_response = client.create_secret(
            Name=secret_name,
            Description=description,
            SecretString=json.dumps(secret_value),
        )
        # If no exception was thrown and we get back the name we passed in the
        # response then the secret was stored successfully.
        return_name = create_secret_value_response["Name"]
        hdbg.dassert_isinstance(return_name, str)
        res: bool = create_secret_value_response["Name"] == secret_name
        return res
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            # Let user know the secret with this name already exists.
            raise ValueError(
                "Secret with this name already exists:", secret_name
            ) from e
        # If not yet implemented handler then just re-raise.
        raise e
    # If we did not return inside try block then something went wrong.
    return False


# TODO(Juraj): this might be deprecated since this is only fit for exchange API keys
def dassert_valid_secret(secret_id: str) -> None:
    """
    The valid format is `exchange_id.stage.account_type.num`.
    """
    values = secret_id.split(".")
    hdbg.dassert_eq(len(values), 4)
    hdbg.dassert_in(
        values[0],
        [
            "binance",
            "bitfinex",
            "coinbase",
            "coinbaseprime",
            "coinbasepro",
            "ftx",
            "gateio",
            "huobi",
            "kraken",
            "kucoin",
            "test",
        ],
    )
    hdbg.dassert_in(values[1], ["local", "preprod"])
    hdbg.dassert_in(values[2], ["trading", "sandbox"])
    hdbg.dassert_is(values[3].isnumeric(), True)
