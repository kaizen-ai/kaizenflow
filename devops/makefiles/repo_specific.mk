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
# Docker.
# #############################################################################

# Pull all the needed images from the registry.
docker_pull:
	docker pull $(IMAGE)
	docker pull $(DEV_TOOLS_IMAGE_PROD)

# Run bash inside container.
# TODO(gp): Move all the compose files under compose/
docker_bash:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/docker-compose-user-space.yml \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash

# Start a container and run the script inside with activated environment.
docker_cmd:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/docker-compose-user-space.yml \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		$(CMD)

# Run jupyter notebook server.
J_PORT?=9999
docker_jupyter:
	J_PORT=$(J_PORT) \
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/docker-compose-jupyter.yml \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server

# #############################################################################
# Run tests on latest image.
# #############################################################################

# Run fast tests locally.
run_fast_tests:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_fast_tests.sh

# Run slow tests.
test_slow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions tests.
# #############################################################################

# Run fast tests.
test_fast_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions release candidate tests.
# #############################################################################
# Test using release candidate image via GH Actions.

# Run fast tests.
test_fast_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app \
		devops/docker_build/run_superslow_tests.sh
