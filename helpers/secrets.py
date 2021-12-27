"""
Import as:

import helpers.secrets as hsec
"""

# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/

import boto3
from botocore.exceptions import ClientError
from botocore.client import SecretsManager
import helpers.dbg as hdbg

# So far we only store the API keys to exchange
DEFAULT_SECRET = "exchange_keys"

def get_secrets_client(region_name="eu-north-1") -> SecretsManager:
    hdbg.dassert_isinstance(region_name, str)
    session = boto3.session.Session()
    client = session.client(
        service_name=region_name,
        region_name="secretsmanager"
    )
    return client

def get_secret(secret_name=DEFAULT_SECRET) -> dict:
    hdbg.dassert_isinstance(secret, str)

    # Create a Secrets Manager client
    client = get_client()

    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # for the full list of exceptions
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return get_secret_value_response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Let user know the secret does not exist
            raise e("No such secret:", secret_name)


#def get_secret_value(secret_name=DEFAULT_SECRET)
