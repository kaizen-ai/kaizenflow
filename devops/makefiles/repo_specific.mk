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

# #############################################################################
# GH Actions
# #############################################################################

# Run fast tests.
docker_fast_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm fast_tests_gh_action

# Run slow tests.
docker_slow_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm slow_tests_gh_action

# Run superslow tests.
docker_superslow_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm superslow_tests_gh_action

# #############################################################################
# Tests
# #############################################################################
# Run fast tests locally.
test_fast:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions tests.
# #############################################################################

# Run fast tests.
test_fast_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions release candidate tests.
# #############################################################################
# Tests using release candidate image via GH Actions.

# Run fast tests.
test_fast_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh
