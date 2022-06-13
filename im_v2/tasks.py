#
# //amp/im/tasks.py
#
import logging
import re

# We inline the code here since we need to make it visible to `invoke`,
# although `from ... import *` is a despicable approach.
from im_lib_tasks import *  # noqa: F403 (unable to detect undefined names)
import helpers.hsecrets as hsecret

_LOG = logging.getLogger(__name__)

@task
def docker_create_candidate_image():
    return

def update_task_definition(task_definition: str, image_hash: str) -> None:
    """
    Create the new revision of the task definition with the new hash(?)
    """
    client = hsecret.get_ecs_client("ck")
    # Get the last revison of the task definition.
    task_def = client.describe_task_definition(
    taskDefinition=task_definition
    )
    task_def = task_def["taskDefinition"]
    old_image = task_def["containerDefinitions"][0]["image"]
    # Edit container version, e.g. `cmamp:prod-12a45` - > cmamp:prod-12b46`
    new_image = re.sub("prod-(.+)$", f"prod-{image_hash}", old_image)
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
        memory=task_def["memory"]
    )
    return response

