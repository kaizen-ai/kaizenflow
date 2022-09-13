"""
Import as:

import helpers.haws as haws
"""

import logging

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3

_LOG = logging.getLogger(__name__)


# #############################################################################
# Utils
# #############################################################################


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


def get_service_client(aws_profile: str, service_name: str) -> BaseClient:
    """
    Return client to work with desired service in the specific region.
    """
    session = get_session(aws_profile)
    client = session.client(service_name=service_name)
    return client


def get_service_resource(aws_profile: str, service_name: str) -> ServiceResource:
    """
    Return resource to work with desired service in the specific region.
    """
    session = get_session(aws_profile)
    resource = session.resource(service_name=service_name)
    return resource


# #############################################################################
# ECS
# #############################################################################


# TODO(Toma): Deprecate in favor of `get_service_client`.
def get_ecs_client(aws_profile: str) -> BaseClient:
    """
    Return client to work with Elastic Container Service in the specific
    region.
    """
    session = get_session(aws_profile)
    client = session.client(service_name="ecs")
    return client


def get_task_definition_image_url(task_definition_name: str) -> str:
    """
    Get ECS task definition by name and return only image URL.

    :param task_definition_name: the name of the ECS task definition, e.g., cmamp-test
    """
    aws_profile = "ck"
    service_name = "ecs"
    with get_service_client(aws_profile, service_name) as client:
        # Get the last revision of the task definition.
        task_description = client.describe_task_definition(
            taskDefinition=task_definition_name
        )
        task_definition_json = task_description["taskDefinition"]
        image_url = task_definition_json["containerDefinitions"][0]["image"]
    return image_url


# TODO(Nikola): Pass a dict config instead, so any part can be updated.
def update_task_definition(task_definition_name: str, new_image_url: str) -> None:
    """
    Create the new revision of specified ECS task definition.

    :param task_definition_name: the name of the ECS task definition for which
        an update to container image URL is made, e.g., cmamp-test
    :param new_image_url: New image url for task definition.
        e.g., `***.dkr.ecr.***/cmamp:prod`
    """
    client = get_ecs_client("ck")
    # Get the last revision of the task definition.
    task_description = client.describe_task_definition(
        taskDefinition=task_definition_name
    )
    task_definition_json = task_description["taskDefinition"]
    # Set new image.
    old_image_url = task_definition_json["containerDefinitions"][0]["image"]
    if old_image_url == new_image_url:
        _LOG.info(
            "New image url `%s` is already set for task definition `%s`!",
            new_image_url,
            task_definition_name,
        )
        return
    task_definition_json["containerDefinitions"][0]["image"] = new_image_url
    # Register the new revision with the new image.
    response = client.register_task_definition(
        family=task_definition_name,
        taskRoleArn=task_definition_json.get("taskRoleArn", ""),
        executionRoleArn=task_definition_json["executionRoleArn"],
        networkMode=task_definition_json["networkMode"],
        containerDefinitions=task_definition_json["containerDefinitions"],
        volumes=task_definition_json["volumes"],
        placementConstraints=task_definition_json["placementConstraints"],
        requiresCompatibilities=task_definition_json["requiresCompatibilities"],
        cpu=task_definition_json["cpu"],
        memory=task_definition_json["memory"],
    )
    updated_image_url = response["taskDefinition"]["containerDefinitions"][0][
        "image"
    ]
    # Check if the image URL is updated.
    hdbg.dassert_eq(updated_image_url, new_image_url)
    _LOG.info(
        "The image URL of `%s` task definition is updated to `%s`",
        task_definition_name,
        updated_image_url,
    )
