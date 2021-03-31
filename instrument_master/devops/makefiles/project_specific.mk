# #############################################################################
# Setup
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com

IM_REPO_BASE_PATH=$(ECR_BASE_PATH)/im
IM_IMAGE_DEV=$(IM_REPO_BASE_PATH):latest
IM_IMAGE_RC=$(IM_REPO_BASE_PATH):rc

NO_SUPERSLOW_TESTS='True'

IM_PG_PORT_LOCAL?=5550

im.print_setup:
	@echo "IM_REPO_BASE_PATH=$(IM_REPO_BASE_PATH)"
	@echo "IM_IMAGE_DEV=$(IM_IMAGE_DEV)"
	@echo "IM_IMAGE_RC=$(IM_IMAGE_RC)"

# #############################################################################
# Development.
# #############################################################################

# Run app container, start a local PostgreSQL DB.
im.docker_up.local:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.local.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		bash

# Run app container w/o PostgreSQL.
im.docker_bash:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.local.yml \
		run \
		--rm \
		-l user=$(USER) \
		--no-deps \
		--entrypoint=instrument_master/devops/docker_build/entrypoints/entrypoint_app_only.sh \
		app \
		bash

# Stop local container including all dependencies.
im.docker_down.local:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.local.yml \
		down

# Stop local container including all dependencies and remove all data.
im.docker_rm.local:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.local.yml \
		down; \
	docker volume rm compose_im_postgres_data_local

im.docker_pull:
	docker pull $(IM_IMAGE_DEV)

# #############################################################################
# Test im workflow (including PostgreSQL server).
# #############################################################################

im.run_fast_tests:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
 		instrument_master/devops/docker_scripts/run_fast_tests.sh

im.run_slow_tests:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		instrument_master/devops/docker_scripts/run_slow_tests.sh

im.run_superslow_tests:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f vendors_amp/devops/compose/docker-compose.yml \
		-f vendors_amp/devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		instrument_master/devops/docker_scripts/run_superslow_tests.sh

# #############################################################################
# Images workflows.
# #############################################################################

ifdef GITHUB_SHA
IMAGE_RC_SHA:=$(GITHUB_SHA)
else
# GITHUB_SHA not found. Setting IMAGE_RC_SHA from HEAD.
IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
endif

# Use Docker buildkit or not.
# DOCKER_BUILDKIT=1
DOCKER_BUILDKIT=0

im.docker_build_image.rc:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IM_IMAGE_RC) \
		-t $(IM_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		--file vendors_amp/devops/docker_build/dev.Dockerfile \
		.

im.docker_build_image_with_cache.rc:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		-t $(IM_IMAGE_RC) \
		-t $(IM_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		--file vendors_amp/devops/docker_build/dev.Dockerfile \
		.

# Push the "rc" image to the registry.
im.docker_push_image.rc:
	docker push $(IM_IMAGE_RC)
	docker push $(IM_REPO_BASE_PATH):$(IMAGE_RC_SHA)

# Mark the "rc" image as "latest".
im.docker_tag_rc_image.latest:
	docker tag $(IM_IMAGE_RC) $(IM_IMAGE_DEV)

# Push the "latest" image to the registry.
im.docker_push_image.latest:
	docker push $(IM_IMAGE_DEV)

im.docker_release.latest:
	make im.docker_build_image_with_cache.rc
	make im.run_fast_tests.rc
	#make run_slow_tests.rc
	make im.docker_tag_rc_image.latest
	make im.docker_push_image.latest
	@echo "==> SUCCESS <=="

#BASE_IMAGE?=$(IM_IMAGE)
#VERSION?=
## Tag :latest image with specific tag
#docker_im_tag_latest_version:
#ifeq ($(VERSION),)
#	@echo "You need to provide VERSION parameter. Example: 'make docker_tag_im_rc_version VERSION=0.1'"
#else
#	docker tag $(BASE_IMAGE) $(IM_REPO_BASE_PATH):$(VERSION)
#endif
#
## Push image wish specific tag
#docker_im_push_version_image:
#ifeq ($(VERSION),)
#	@echo "You need to provide VERSION parameter. Example: 'make docker_push_im_version_image VERSION=0.1'"
#else
#	docker push $(IM_REPO_BASE_PATH):$(VERSION)
#endif
