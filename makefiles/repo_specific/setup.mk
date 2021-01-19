# #############################################################################
# Setup.
# #############################################################################
ECR_URL=https://083233266530.dkr.ecr.us-east-2.amazonaws.com
DEV_TOOLS_PROD_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod
PARTICLE_ENV_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/particle_env:latest
IMAGE?=$(PARTICLE_ENV_IMAGE)
REPO_IMAGES=$(PARTICLE_ENV_IMAGE) $(DEV_TOOLS_PROD_IMAGE)
