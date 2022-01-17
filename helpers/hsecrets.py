"""
Import as:

import helpers.hsecrets as hsecret
"""

import json
from typing import Optional, Any, Dict

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

import helpers.hdbg as hdbg


def get_secrets_client(*, aws_profile: str = 'ck') -> BaseClient:
    """
    Return client to work with AWS Secrets Manager in the specified region.
    """
    hdbg.dassert_isinstance(aws_profile, str)
    session = boto3.session.Session(profile_name=aws_profile)
    client = session.client(
        service_name="secretsmanager"
    )
    return client


# TODO(Juraj): add support to access secrets for different profiles, not important rn
def get_secret(secret_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch secret values(s) from AWS secrets manager, returns a dictionary of
    key-value pairs. example: get_secret('binance') returns:

    { 'apiKey': '<secret_value>', 'secret': '<secret_value>' }
    """
    hdbg.dassert_isinstance(secret_name, str)
    # Create a AWS Secrets Manager client.
    client = get_secrets_client()
    # Stores value of retrieved secret.
    secret_val = {}
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # for the full list of exceptions.
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response["SecretString"]
        hdbg.dassert_isinstance(secret_string, str)
        secret_val = json.loads(secret_string)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # Let user know the secret does not exist.
            raise ValueError("No such secret:", secret_name) from e
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
    client = get_secrets_client()

    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_CreateSecret.html
    # for the full list of exceptions
    try:
        create_secret_value_response = client.create_secret(
            Name=secret_name,
            Description=description,
            SecretString=json.dumps(secret_value),
        )

        # If no exception was thrown and we get back the name we passed in the response
        # then the secret was stored successfully
        return_name = create_secret_value_response["Name"]
        hdbg.dassert_isinstance(return_name, str)
        return create_secret_value_response["Name"] == secret_name
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            # Let user know the secret with this name already exists
            raise ValueError(
                "Secret with this name already exists:", secret_name
            ) from e
        # If not yet implemented handler then just re-raise.
        raise e

    # If we did not return inside try block then something went wrong
    return False
