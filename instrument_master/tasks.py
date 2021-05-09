#
# //amp/instrument_master/tasks.py
#
import logging

# We inline the code here since we need to make it visible to `invoke`,
# although `from ... import *` is a despicable approach.
from lib_tasks import *  # noqa: F403 (unable to detect undefined names)

_LOG = logging.getLogger(__name__)

# #############################################################################
# Setup.
# #############################################################################

# TODO(gp): We could move this into lib_tasks.py if really constant.
ECR_BASE_PATH = "665840871993.dkr.ecr.us-east-1.amazonaws.com"


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a
    # different image, e.g., `XYZ_tmp` to not interfere with the prod system:
    # "BASE_IMAGE": "..._tmp",
    "BASE_IMAGE": "im",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod",
    "DOCKER_COMPOSE_FILES": None,
}


set_default_params(
    default_params
)  # noqa: F405 (may be or defined from star imports)
