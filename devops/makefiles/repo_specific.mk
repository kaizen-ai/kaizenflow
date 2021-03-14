# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com
# TODO(gp): -> amp
ECR_REPO_BASE_PATH:=$(ECR_BASE_PATH)/amp_env

AMP_IMAGE_DEV=$(ECR_REPO_BASE_PATH):latest
AMP_IMAGE_RC=$(ECR_REPO_BASE_PATH):rc

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
