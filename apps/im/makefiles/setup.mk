# #############################################################################
# Setup
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com

IM_REPO_BASE_PATH=$(ECR_BASE_PATH)/im
IM_IMAGE_DEV=$(IM_REPO_BASE_PATH):latest
IM_IMAGE_RC=$(IM_REPO_BASE_PATH):rc

NO_SUPERSLOW_TESTS='True'

IM_PG_PORT_LOCAL?=5432

im-app.print_setup:
	@echo "IM_REPO_BASE_PATH=$(IM_REPO_BASE_PATH)"
	@echo "IM_IMAGE_DEV=$(IM_IMAGE_DEV)"
	@echo "IM_IMAGE_RC=$(IM_IMAGE_RC)"

