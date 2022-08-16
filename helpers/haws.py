"""
Import as:

import helpers.haws as haws
"""

import logging

import boto3
from botocore.client import BaseClient

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3

_LOG = logging.getLogger(__name__)

# TODO(Toma): to abstract the function to be able to instantiate client for
#  any service so we don't have duplicate code.
def get_ecs_client(aws_profile: str) -> BaseClient:
    """
    Return client to work with Elastic Container Service in the specific
    region.
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
    credentials = hs3.get_aws_credentials(aws_profile=aws_profile)
    credentials = credentials.copy()
    # Boto session expects `region_name`.
    credentials["region_name"] = credentials.pop("aws_region")
    # TODO(gp): a better approach is to just extract what boto.Session needs rather
    #  then passing everything.
    if "aws_s3_bucket" in credentials:
        del credentials["aws_s3_bucket"]
    _LOG.debug(hprint.to_str("credentials"))
    session = boto3.session.Session(**credentials)
    return session
