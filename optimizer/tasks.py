import logging
import os

import helpers.hversion as hversi
import helpers.lib_tasks as hlib

# Expose the pytest targets.
# Extract with:
# > i print_tasks --as-code
from opt_lib_tasks import (
    docker_build_local_opt_image,
    opt_docker_bash,
    opt_docker_jupyter,
)

_LOG = logging.getLogger(__name__)


# #############################################################################
# Setup.
# #############################################################################
#
#
# TODO(gp): Move it to lib_tasks.
ECR_BASE_PATH = os.environ["AM_ECR_BASE_PATH"]


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "opt_tmp",
    "BASE_IMAGE": "opt",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod",
    "USE_ONLY_ONE_DOCKER_COMPOSE": True,
}


hlib.set_default_params(default_params)
