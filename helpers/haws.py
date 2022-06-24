"""
Import as:

import helpers.haws as haws
"""

import boto3
from botocore.client import BaseClient

import helpers.hdbg as hdbg
import helpers.hs3 as hs3


def get_ecs_client(aws_profile: str) -> BaseClient:
    """
    Return client to work with Elastic Client Service in the specific region.
    """
    session = get_session(aws_profile)
    client = session.client(service_name="ecs")
    return client


def get_session(aws_profile: str) -> boto3.session.Session:
    """
    Return connected Boto3 session.
    """
    hdbg.dassert_isinstance(aws_profile, str)
    # Original credentials are cached, thus we do not want to edit them.
    credentials = {**hs3.get_aws_credentials(aws_profile=aws_profile)}
    # Boto session expects `region_name`.
    credentials["region_name"] = credentials.pop("aws_region")
    session = boto3.session.Session(**credentials)
    return session
