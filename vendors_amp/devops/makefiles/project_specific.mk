# #############################################################################
# Setup
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com

KIBOT_REPO_BASE_PATH=$(ECR_BASE_PATH)/im
KIBOT_IMAGE_DEV=$(KIBOT_REPO_BASE_PATH):latest
KIBOT_IMAGE_RC=$(KIBOT_REPO_BASE_PATH):rc

NO_SUPERSLOW_TESTS='True'

IM_PG_PORT_LOCAL?=5550

im.print_setup:
	@echo "KIBOT_REPO_BASE_PATH=$(KIBOT_REPO_BASE_PATH)"
	@echo "KIBOT_IMAGE_DEV=$(KIBOT_IMAGE_DEV)"
	@echo "KIBOT_IMAGE_RC=$(KIBOT_IMAGE_RC)"

# #############################################################################
# Development.
# #############################################################################

im.docker_bash:
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	IMAGE=$(KIBOT_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.local.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		bash

im.docker_pull:
	docker pull $(KIBOT_IMAGE_DEV)

# #############################################################################
# Test kibot workflow (including PostgreSQL server).
# #############################################################################

im.run_fast_tests:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
 		vendors_amp/devops/docker_scripts/run_fast_tests.sh

im.run_slow_tests:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		vendors_amp/devops/docker_scripts/run_slow_tests.sh

im.run_superslow_tests:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.test.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		vendors_amp/devops/docker_scripts/run_superslow_tests.sh

# #############################################################################
# Services.
# #############################################################################

# Start local postgres server.
im.docker_postgres_up.local:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.local.yml \
		up \
		--remove-orphans

# Stop local postgres server.
im.docker_postgres_down.local:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.local.yml \
		down \
		--remove-orphans

# Stop local postgres server and remove all data.
im.docker_postgres_rm.local:
	IMAGE=$(KIBOT_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.local.yml \
		down \
		--remove-orphans; \
	docker volume rm compose_kibot_postgres_data_local

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
		-t $(KIBOT_IMAGE_RC) \
		-t $(KIBOT_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		--file devops/docker_build/dev.Dockerfile \
		.

im.docker_build_image_with_cache.rc:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		-t $(KIBOT_IMAGE_RC) \
		-t $(KIBOT_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		--file devops/docker_build/dev.Dockerfile \
		.

# Push the "rc" image to the registry.
im.docker_push_image.rc:
	docker push $(KIBOT_IMAGE_RC)
	docker push $(KIBOT_REPO_BASE_PATH):$(IMAGE_RC_SHA)

# Mark the "rc" image as "latest".
im.docker_tag_rc_image.latest:
	docker tag $(KIBOT_IMAGE_RC) $(KIBOT_IMAGE_DEV)

# Push the "latest" image to the registry.
im.docker_push_image.latest:
	docker push $(KIBOT_IMAGE_DEV)

im.docker_release.latest:
	make im.docker_build_image_with_cache.rc
	make im.run_fast_tests.rc
	#make run_slow_tests.rc
	make im.docker_tag_rc_image.latest
	make im.docker_push_image.latest
	@echo "==> SUCCESS <=="

#BASE_IMAGE?=$(KIBOT_IMAGE)
#VERSION?=
## Tag :latest image with specific tag
#docker_kibot_tag_latest_version:
#ifeq ($(VERSION),)
#	@echo "You need to provide VERSION parameter. Example: 'make docker_tag_kibot_rc_version VERSION=0.1'"
#else
#	docker tag $(BASE_IMAGE) $(KIBOT_REPO_BASE_PATH):$(VERSION)
#endif
#
## Push image wish specific tag
#docker_kibot_push_version_image:
#ifeq ($(VERSION),)
#	@echo "You need to provide VERSION parameter. Example: 'make docker_push_kibot_version_image VERSION=0.1'"
#else
#	docker push $(KIBOT_REPO_BASE_PATH):$(VERSION)
#endif
