"""
Import as:

import helpers.hboto3 as hboto3
"""

import argparse
import logging
import re

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


def _update_task_definition(task_definition: str, image_tag: str) -> None:
    """
    Create the new revision of specified ECS task definition and point Image
    URL specified to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update
    to container image URL is made, e.g. cmamp-test
    :param image_tag: the hash of the new candidate image, e.g. 13538588e
    """
    client = haws.get_ecs_client("ck")
    # Get the last revison of the task definition.
    task_description = client.describe_task_definition(
        taskDefinition=task_definition
    )
    task_def = task_description["taskDefinition"]
    old_image = task_def["containerDefinitions"][0]["image"]
    # Edit container version, e.g. cmamp:prod-12a45 - > cmamp:prod-12b46`
    new_image = re.sub("prod-(.+)$", f"prod-{image_tag}", old_image)
    task_def["containerDefinitions"][0]["image"] = new_image
    # Register the new revision with the new image.
    response = client.register_task_definition(
        family=task_definition,
        taskRoleArn=task_def["taskRoleArn"],
        executionRoleArn=task_def["taskRoleArn"],
        networkMode=task_def["networkMode"],
        containerDefinitions=task_def["containerDefinitions"],
        volumes=task_def["volumes"],
        placementConstraints=task_def["placementConstraints"],
        requiresCompatibilities=task_def["requiresCompatibilities"],
        cpu=task_def["cpu"],
        memory=task_def["memory"],
    )
    new_image_url = response["taskDefinition"]["containerDefinitions"][0]["image"]
    # Check if the image URL is updated.
    hdbg.dassert_eq(new_image_url.endswith(image_tag), True)
    _LOG.info(
        "The image URL of `%s` task definition is updated to `%s`",
        task_definition,
        new_image_url,
    )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-t",
        "--task_definition",
        type=str,
        help="the name of the ECS task definition to update",
    )
    parser.add_argument(
        "-i",
        "--image_tag",
        type=str,
        help="the hash of the new candidate image",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _update_task_definition(args.task_definition, args.image_tag)


if __name__ == "__main__":
    _main(_parse())
