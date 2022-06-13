#
# //amp/im/tasks.py
#
from ctypes.wintypes import tagPOINT
import logging
import re

# We inline the code here since we need to make it visible to `invoke`,
# although `from ... import *` is a despicable approach.
from im_lib_tasks import *  # noqa: F403 (unable to detect undefined names)

import helpers.hsecrets as hsecret
import helpers.hversion as hversio
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

@task
def docker_create_candidate_image(task_definition: str):  # type: ignore
    """
    """
    # Get latest version.
    last_version = hversio.get_changelog_version(".")
    # Create new prod image.
    cmd = f"invoke docker_build_prod_image -v {last_version} --candidate"
    rc = hsystem.system_to_string(cmd, abort_on_error=False)
    hdbg.dassert_eq(rc[0], 0)
    # Extract the image tag from the output log.
    tag = _extract_image_hash(rc[1])
    # Push candidate image.
    cmd = f"invoke docker_push_prod_candidate_image --candidate {tag}"
    rc = hsystem.system(cmd, abort_on_error=False)
    hdbg.dassert_eq(rc, 0)
    # Register new task definition revision with updated image URL. 
    update_task_definition(task_definition, tag)
    return

def _extract_image_hash(output: str) -> str:
    """
    Extract prod image hash from the output log.

    :param output: output of `i docker_build_prod_image`
    :return: image hash, e.g. `13538588e`
    """
    # Extract image hash, it is stored in the last line of the output.
    lines = output[1].split("\n")
    # Split last output line into columns, get `TAG` column.
    tag = lines[-1].split("   ")[1]
    # Extract hash, e.g. 'prod-13538588e' -> `13538588e`.
    tag = tag.split("-")[1]
    return tag

def update_task_definition(task_definition: str, image_tag: str) -> None:
    """
    Create the new revision of the task definition with the new image tag.

    :param task_definition: the name of the task, e.g. `cmamp-test`
    :param image_tag: image tag, e.g. `13538588e`
    """
    client = hsecret.get_ecs_client("ck")
    # Get the last revison of the task definition.
    task_description = client.describe_task_definition(
    taskDefinition=task_definition
    )
    task_def = task_description["taskDefinition"]
    old_image = task_def["containerDefinitions"][0]["image"]
    # Edit container version, e.g. `cmamp:prod-12a45` - > cmamp:prod-12b46`
    new_image = re.sub("prod-(.+)$", f"prod-{image_tag}", old_image)
    task_def["containerDefinitions"][0]["image"] = new_image
    # Register the new revision with the new image.
    client.register_task_definition(
        family=task_definition,
        taskRoleArn=task_def["taskRoleArn"],
        executionRoleArn=task_def["taskRoleArn"],
        networkMode=task_def["networkMode"],
        containerDefinitions=task_def["containerDefinitions"],
        volumes=task_def["volumes"],
        placementConstraints=task_def["placementConstraints"],
        requiresCompatibilities=task_def["requiresCompatibilities"],
        cpu=task_def["cpu"],
        memory=task_def["memory"]
    )
    return

