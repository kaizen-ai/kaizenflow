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
    # TODO(gp): -> amp?
    "ECR_REPO_BASE_PATH": f"{ECR_BASE_PATH}/amp_env",
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "ECR_REPO_BASE_PATH": f"{ECR_BASE_PATH}/amp_env_tmp",
    # Point to the dir with the run_scripts.
    #"RUN_TESTS_DIR": "devops/docker_scripts",
    "NO_SUPERSLOW_TESTS": True,
}


set_default_params(default_params)
