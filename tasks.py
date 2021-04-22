import logging

_LOG = logging.getLogger(__name__)

# We inline the code here since we need to make it visible to `invoke`, although
# `from ... import *` is bad practice.
from lib_tasks import *

# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH = "665840871993.dkr.ecr.us-east-1.amazonaws.com"

DEV_TOOLS_IMAGE_PROD = f"{ECR_BASE_PATH}/dev_tools:prod"

default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,

    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "amp_tmp",
    "BASE_IMAGE": "amp_env",

    "NO_SUPERSLOW_TESTS": True,
}


set_default_params(default_params)
