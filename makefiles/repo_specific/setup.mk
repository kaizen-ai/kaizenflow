# #############################################################################
# Setup.
# #############################################################################
ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com
ECR_URL=https://$(ECR_BASE_PATH)
ECR_REPO_BASE_PATH:=$(ECR_BASE_PATH)/amp_env
DEV_TOOLS_PROD_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod
AMP_ENV_IMAGE=$(ECR_REPO_BASE_PATH):latest
AMP_ENV_IMAGE_RC=$(ECR_REPO_BASE_PATH):rc
IMAGE=$(AMP_ENV_IMAGE)
IMAGE_RC=$(AMP_ENV_IMAGE_RC)
REPO_IMAGES=$(AMP_ENV_IMAGE) $(DEV_TOOLS_PROD_IMAGE)

setup_print:
	@echo "ECR_BASE_PATH=$(ECR_BASE_PATH)"
	@echo "ECR_URL=$(ECR_URL)"
	@echo "ECR_REPO_BASE_PATH=$(ECR_REPO_BASE_PATH)"
	@echo "DEV_TOOLS_PROD_IMAGE=$(DEV_TOOLS_PROD_IMAGE)"
	@echo "AMP_ENV_IMAGE=$(AMP_ENV_IMAGE)"
	@echo "AMP_ENV_IMAGE_RC=$(AMP_ENV_IMAGE_RC)"
	@echo "IMAGE=$(IMAGE)"
	@echo "IMAGE_RC=$(IMAGE_RC)"
	@echo "REPO_IMAGES=$(REPO_IMAGES)"
