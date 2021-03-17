# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com

# TODO(gp): -> amp
ECR_REPO_BASE_PATH:=$(ECR_BASE_PATH)/amp_env
# When testing a change to the build system in a branch you can use a different
# image, e.g., `amp_tmp` to not interfere with the prod system.
#ECR_REPO_BASE_PATH:=$(ECR_REPO_BASE_PATH)_tmp

AMP_IMAGE_DEV=$(ECR_REPO_BASE_PATH):latest
AMP_IMAGE_RC=$(ECR_REPO_BASE_PATH):rc

# Target image for the common actions.
IMAGE_DEV=$(AMP_IMAGE_DEV)
IMAGE_RC=$(AMP_IMAGE_RC)

DEV_TOOLS_IMAGE_PROD=$(ECR_BASE_PATH)/dev_tools:prod

print_setup:
	@echo "ECR_BASE_PATH=$(ECR_BASE_PATH)"
	@echo "ECR_REPO_BASE_PATH=$(ECR_REPO_BASE_PATH)"
	@echo "DEV_TOOLS_IMAGE_PROD=$(DEV_TOOLS_IMAGE_PROD)"
	@echo "IMAGE_DEV=$(IMAGE_DEV)"
	@echo "IMAGE_RC=$(IMAGE_RC)"
	
# #############################################################################
# Docker development.
# #############################################################################

# Pull all the needed images from the registry.
docker_pull:
	docker pull $(IMAGE_DEV)
	docker pull $(DEV_TOOLS_IMAGE_PROD)
