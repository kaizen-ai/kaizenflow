import logging

# We inline the code here since we need to make it visible to `invoke`, although
# `from ... import *` is bad practice.
from lib_tasks import *

_LOG = logging.getLogger(__name__)

# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH = "665840871993.dkr.ecr.us-east-1.amazonaws.com"


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "..._tmp",
    "BASE_IMAGE": "im_tws",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod"
}


set_default_params(default_params)

# # Build images.
# # # --no-cache
# ib_connect.docker_build_image.rc:
# 	DOCKER_BUILDKIT=0 \
# 	docker build \
# 		--progress=plain \
# 		-t $(IB_CONNECT_RC_IMAGE) \
# 		-t $(IB_CONNECT_RC_IMAGE_SHA) \
# 		--file devops/docker_build/Dockerfile \
# 		.
