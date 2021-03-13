# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com
# TODO(gp): -> amp
ECR_REPO_BASE_PATH:=$(ECR_BASE_PATH)/amp_env

# TODO(gp): -> AMP_IMAGE_DEV
AMP_ENV_IMAGE_DEV=$(ECR_REPO_BASE_PATH):latest
# TODO(gp): -> AMP_IMAGE_RC
AMP_ENV_IMAGE_RC=$(ECR_REPO_BASE_PATH):rc

DEV_TOOLS_IMAGE_PROD=$(ECR_BASE_PATH)/dev_tools:prod

# TODO(gp): -> IMAGE_DEV
IMAGE=$(AMP_ENV_IMAGE_DEV)
IMAGE_RC=$(AMP_ENV_IMAGE_RC)

print_setup:
	@echo "ECR_BASE_PATH=$(ECR_BASE_PATH)"
	@echo "ECR_REPO_BASE_PATH=$(ECR_REPO_BASE_PATH)"
	@echo "DEV_TOOLS_IMAGE_PROD=$(DEV_TOOLS_IMAGE_PROD)"
	@echo "IMAGE=$(IMAGE)"
	@echo "IMAGE_RC=$(IMAGE_RC)"
	
# #############################################################################
# Docker development.
# #############################################################################

# Pull all the needed images from the registry.
docker_pull:
	docker pull $(IMAGE)
	docker pull $(DEV_TOOLS_IMAGE_PROD)

# Run bash inside container.
docker_bash:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose-user-space.yml \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash

# Start a container and run the script inside with activated environment.
docker_cmd:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose-user-space.yml \
		run \
		--rm \
		-l user=$(USER) \
		--entrypoint $(CMD) \
		user_space \
		$(CMD)

# Run jupyter notebook server.
J_PORT?=9999
docker_jupyter:
	J_PORT=$(J_PORT) \
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose-jupyter.yml \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server

# #############################################################################
# Run tests with "latest" image.
# #############################################################################

# TODO(gp): Move all this to the general.mk, once it's unified.

# The user can use IMAGE_RC to test a change to the build system.
# > make run_*_tests IMAGE=083233266530.dkr.ecr.us-east-2.amazonaws.com/amp_env:rc

# We need to pass the params from the callers.
_run_tests:
	IMAGE=$(_IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose-user-space.yml \
		run \
		-l user=$(USER) \
		--rm \
		user_space \
		$(_CMD)

# Make sure pytest works.
run_blank_tests:
	_IMAGE=$(IMAGE) \
	_CMD="pytest -h >/dev/null" \
	make _run_tests

# Run fast tests locally.
run_fast_tests:
	_IMAGE=$(IMAGE) \
	_CMD="devops/docker_scripts/run_fast_tests.sh" \
	make _run_tests

# Run slow tests.
run_slow_tests:
	_IMAGE=$(IMAGE) \
	_CMD="devops/docker_scripts/run_slow_tests.sh" \
	make _run_tests

# Run superslow tests.
run_superslow_tests:
	_IMAGE=$(IMAGE) \
	_CMD="devops/docker_scripts/run_superslow_tests.sh" \
	make _run_tests

# #############################################################################
# Run tests with "rc" image.
# #############################################################################

# Make sure pytest works.
run_blank_tests.rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="pytest -h >/dev/null" \
	make _run_tests

# TODO: Move the *.sh to docker_scripts

# Run fast tests locally.
run_fast_tests.rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_fast_tests.sh" \
	make _run_tests

# Run slow tests.
run_slow_tests.rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_slow_tests.sh" \
	make _run_tests

# Run superslow tests.
run_superslow_tests.rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_superslow_tests.sh" \
	make _run_tests

# #############################################################################
# GH actions tests for "latest" image.
# #############################################################################

_run_tests.gh_action:
	IMAGE=$(_IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		$(_CMD)

run_fast_tests.gh_action:
	_IMAGE=$(IMAGE)
	_CMD="devops/docker_scripts/run_fast_tests.sh" \
	make _run_tests.gh_action

run_slow_tests.gh_action:
	_IMAGE=$(IMAGE)
	_CMD="devops/docker_scripts/run_slow_tests.sh" \
	make _run_tests.gh_action

run_superslow_tests.gh_action:
	_IMAGE=$(IMAGE)
	_CMD="devops/docker_scripts/run_superslow_tests.sh" \
	make _run_tests.gh_action

# #############################################################################
# GH actions tests for "rc" image.
# #############################################################################

# Test using release candidate image via GH Actions.

run_fast_tests.gh_action_rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_fast_tests.sh" \
	make _run_tests.gh_action

run_slow_tests.gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_slow_tests.sh" \
	make _run_tests.gh_action

run_superslow_tests.gh_action_rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="devops/docker_scripts/run_superslow_tests.sh" \
	make _run_tests.gh_action
