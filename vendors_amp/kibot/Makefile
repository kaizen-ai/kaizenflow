# #############################################################################
# Setup
# #############################################################################
ifdef $(GITHUB_SHA)
IMAGE_RC_SHA:=$(GITHUB_SHA)
else
# GITHUB_SHA not found. Setting up IMAGE_RC_SHA form HEAD.
IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
endif
KIBOT_REPO_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com/kibot
KIBOT_IMAGE=$(KIBOT_REPO_BASE_PATH):latest
KIBOT_IMAGE_RC=$(KIBOT_REPO_BASE_PATH):rc
KIBOT_IMAGE_RC_SHA=$(KIBOT_REPO_BASE_PATH):$(IMAGE_RC_SHA)

# #############################################################################
# PostgreSQL server workflow.
# #############################################################################

# Start local postgres server.
docker_kibot_up_postgres_local:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		up \
		-d \
		kibot_postgres_local

# Stop local postgres server.
docker_kibot_down_postgres_local:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		down

# Stop local postgres server and remove all data.
docker_kibot_rm_postgres_local:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		down \
		-v


# #############################################################################
# Kibot workflow (including PostgreSQL server).
# #############################################################################

# Run container with local database.
# Use for development.
docker_kibot_run_local:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app bash


# #############################################################################
# Test kibot workflow (including PostgreSQL server).
# #############################################################################

# Run container with local database.
# Run fast tests.
docker_kibot_fast_tests:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.test.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app \
		bash run_fast_tests.sh

# Run container with local database.
# Run slow tests.
docker_kibot_slow_tests:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.test.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app \
		bash run_slow_tests.sh

# Run container with local database.
# Run superslow tests.
docker_kibot_superslow_tests:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.test.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app \
		bash run_superslow_tests.sh

# #############################################################################
# Docker workflow.
# #############################################################################
kibot_setup_print:
	@echo "KIBOT_REPO_BASE_PATH=$(KIBOT_REPO_BASE_PATH)"
	@echo "KIBOT_IMAGE=$(KIBOT_IMAGE)"
	@echo "KIBOT_IMAGE_RC=$(KIBOT_IMAGE_RC)"
	@echo "KIBOT_IMAGE_RC_SHA=$(KIBOT_IMAGE_RC_SHA)"

# Pull images from ecr
docker_pull_kibot:
	docker pull $(KIBOT_IMAGE)

# Build images
docker_build_kibot_rc_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(KIBOT_IMAGE_RC) \
		-t $(KIBOT_IMAGE_RC_SHA) \
		--file ./Dockerfile .

# Push release candidate images
docker_push_kibot_rc_image:
	docker push $(KIBOT_IMAGE_RC)
	docker push $(KIBOT_IMAGE_RC_SHA)

# Tag :rc image with :latest tag
docker_tag_kibot_rc_latest:
	docker tag $(KIBOT_IMAGE_RC) $(KIBOT_IMAGE)

# Push image with :latest tag
docker_push_kibot_latest_image:
	docker push $(KIBOT_IMAGE)


BASE_IMAGE?=$(KIBOT_IMAGE)
VERSION?=
# Tag :latest image with specific tag
docker_tag_kibot_latest_version:
ifeq ($(VERSION),)
	@echo "You need to provide VERSION parameter. Example: 'make docker_tag_kibot_rc_version VERSION=0.1'"
else
	docker tag $(BASE_IMAGE) $(KIBOT_REPO_BASE_PATH):$(VERSION)
endif

# Push image wish specific tag
docker_push_kibot_version_image:
ifeq ($(VERSION),)
	@echo "You need to provide VERSION parameter. Example: 'make docker_push_kibot_version_image VERSION=0.1'"
else
	docker push $(KIBOT_REPO_BASE_PATH):$(VERSION)
endif



