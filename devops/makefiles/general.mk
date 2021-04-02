# #############################################################################
# Development.
# #############################################################################

# Log in to AWS ECR.
AWSCLI_VERSION=$(shell aws --version | awk '{print $$1}' | awk -F"/" '{print $$2}')
AWSCLI_MAJOR_VERSION=$(shell echo "$(AWSCLI_VERSION)" | awk -F"." '{print $$1}')
docker_login:
	@echo AWS CLI version: $(AWSCLI_VERSION)
	@echo AWS CLI major version: $(AWSCLI_MAJOR_VERSION)
ifeq ($(AWSCLI_MAJOR_VERSION), 1)
	eval `aws ecr get-login --no-include-email --region us-east-2`
else
	docker login -u AWS -p $(aws ecr get-login --region us-east-2) https://$(ECR_REPO_BASE_PATH)
endif

# Print all the makefile targets.
targets:
	find . -name "*.mk" -o -name "Makefile" | xargs -n 1 perl -ne 'if (/^\S+:$$/) { print $$_ }'

# Print all the makefiles.
makefiles:
	find . -name "*.mk" -o -name "Makefile" | sort

# List images in the logged in repo.
docker_repo_images:
	docker image ls $(ECR_BASE_PATH)

# List all running containers:
#   ```
#   > docker_ps
#   CONTAINER ID  user  IMAGE                    COMMAND                 	  CREATED        STATUS        PORTS  service
#   2ece37303ec9  gad   083233266530....:latest  "./docker_build/entrâ¦"  5 seconds ago  Up 4 seconds         user_space
#   ```
docker_ps:
	docker ps --format='table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}\t{{.Label "com.docker.compose.service"}}'

# Report container stats, e.g., CPU, RAM.
#   ```
#   > docker_stats
#   CONTAINER ID  NAME                   CPU %  MEM USAGE / LIMIT     MEM %  NET I/O         BLOCK I/O        PIDS
#   2ece37303ec9  ..._user_space_run_30  0.00%  15.74MiB / 31.07GiB   0.05%  351kB / 6.27kB  34.2MB / 12.3kB  4
#   ```
docker_stats:
	# To change output format you can use following --format flag with `docker stats` command.
	# --format='table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
	docker stats --no-stream $(IDS)

# Workaround for when amp is a submodule of another module
#
# See AmpTask1017.
#
# Return the path to the Git repo including the Git submodule for a submodule
# and it's empty for a supermodule.
SUBMODULE_SUPERPROJECT=$(shell git rev-parse --show-superproject-working-tree)
# E.g., `amp`.
SUBMODULE_NAME=$(shell ( \
		git config --file ${SUBMODULE_SUPERPROJECT}/.gitmodules --get-regexp path \
		| grep $(basename "$(pwd)") \
		| awk '{ print $$2 }'))

ifeq ($(SUBMODULE_SUPERPROJECT), )
DOCKER_COMPOSE_USER_SPACE=devops/compose/docker-compose-user-space.yml
else
DOCKER_COMPOSE_USER_SPACE=devops/compose/docker-compose-user-space-git-subrepo.yml
endif

docker_bash:
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash

docker_bash.rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash
	
docker_bash.prod:
ifdef IMAGE_PROD
	IMAGE=$(IMAGE_PROD) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash
else
	@echo "IMAGE_PROD is not defined"
endif

# No need to overwrite the entrypoint:
#	--entrypoint $(CMD)
docker_cmd:
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		'$(CMD)'

docker_cmd.rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		'$(CMD)'

# Run jupyter notebook server.
# E.g., run on port 10000 using a specific image:
# > make docker_jupyter J_PORT=10000 IMAGE="083233266530.dkr.ecr.us-east-2.amazonaws.com/amp_env:rc"
J_PORT?=9999
docker_jupyter:
ifeq ($(NO_JUPYTER), 'True')
	@echo "Jupyter is not supported"
else
	J_PORT=$(J_PORT) \
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		-f devops/compose/docker-compose-jupyter.yml \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server
endif

docker_kill_last:
	docker ps -l
	docker rm -f $(shell docker ps -l -q)

# #############################################################################
# Run tests with "latest" image.
# #############################################################################

print_debug_setup:
	@echo "SUBMODULE_NAME=$(SUBMODULE_NAME)"
	@echo "DOCKER_COMPOSE_USER_SPACE=${DOCKER_COMPOSE_USER_SPACE}"
	@echo "NO_JUPYTER=$(NO_JUPYTER)"
ifeq ($(NO_JUPYTER), 'True')
	@echo "  No Jupyter"
else
	@echo "  Execute Jupyter"
endif
	@echo "NO_FAST_TESTS=$(NO_FAST_TESTS)"
ifeq ($(NO_FAST_TESTS), 'True')
	@echo "  Do not execute fast tests"
else
	@echo "  Execute fast tests"
endif
	@echo "NO_SLOW_TESTS=$(NO_SLOW_TESTS)"
ifeq ($(NO_SLOW_TESTS), 'True')
	@echo "  Do not execute slow tests"
else
	@echo "  Execute slow tests"
endif
	@echo "NO_SUPERSLOW_TESTS=$(NO_SUPERSLOW_TESTS)"
ifeq ($(NO_SUPERSLOW_TESTS), 'True')
	@echo "  Do not execute superslow tests"
else
	@echo "  Execute superslow tests"
endif

# The user can pass another IMAGE to run tests in another image.

# We need to pass the params from the callers.
# E.g.,
# > make run_*_tests _IMAGE=083233266530.dkr.ecr.us-east-2.amazonaws.com/amp_env:rc
_run_tests:
	IMAGE=$(_IMAGE) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		$(_CMD)

# Make sure pytest works.
run_blank_tests:
	_IMAGE=$(IMAGE_DEV) \
	_CMD="pytest -h >/dev/null" \
	make _run_tests

run_fast_tests:
ifeq ($(NO_FAST_TESTS), 'True')
	@echo "No fast tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests
endif

run_slow_tests:
ifeq ($(NO_SLOW_TESTS), 'True')
	@echo "No slow tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests
endif

run_superslow_tests:
ifeq ($(NO_SUPERSLOW_TESTS), 'True')
	@echo "No superslow tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests
endif

# #############################################################################
# Run tests with "rc" image.
# #############################################################################

# Make sure pytest works.
run_blank_tests.rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="pytest -h >/dev/null" \
	make _run_tests

run_fast_tests.rc:
ifeq ($(NO_FAST_TESTS), 'True')
	@echo "No fast tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests
endif

run_slow_tests.rc:
ifeq ($(NO_SLOW_TESTS), 'True')
	@echo "No slow tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests
endif

run_superslow_tests.rc:
ifeq ($(NO_SUPERSLOW_TESTS), 'True')
	@echo "No superslow tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests
endif

# #############################################################################
# GH actions tests for "latest" image.
# #############################################################################

_run_tests.gh_action:
	IMAGE=$(_IMAGE) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		$(_CMD)

run_fast_tests.gh_action:
ifeq ($(NO_FAST_TESTS), 'True')
	@echo "No fast tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests.gh_action
endif

run_slow_tests.gh_action:
ifeq ($(NO_SLOW_TESTS), 'True')
	@echo "No slow tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests.gh_action
endif

run_superslow_tests.gh_action:
ifeq ($(NO_SUPERSLOW_TESTS), 'True')
	@echo "No superslow tests"
else
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests.gh_action
endif

# #############################################################################
# GH actions tests for "rc" image.
# #############################################################################

# Test using release candidate image via GH Actions.

run_fast_tests.gh_action_rc:
ifeq ($(NO_FAST_TESTS), 'True')
	@echo "No fast tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests.gh_action
endif

run_slow_tests.gh_action_rc:
ifeq ($(NO_SLOW_TESTS), 'True')
	@echo "No slow tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests.gh_action
endif

run_superslow_tests.gh_action_rc:
ifeq ($(NO_SUPERSLOW_TESTS), 'True')
	@echo "No superslow tests"
else
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests.gh_action
endif

docker_bash.gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f devops/compose/docker-compose.yml \
		-f devops/compose/docker-compose.gh_actions.yml \
		run \
		--rm \
		-l user=$(USER) \
		app \
		bash

# #############################################################################
# Images workflows.
# #############################################################################

ifdef GITHUB_SHA
IMAGE_RC_SHA:=$(GITHUB_SHA)
else
# GITHUB_SHA not found. Setting IMAGE_RC_SHA from HEAD.
IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
endif
IMAGE_RC?=$(IMAGE_RC)

# Use Docker buildkit or not.
# DOCKER_BUILDKIT=1
DOCKER_BUILDKIT=0

# DEV image flow:
# - A release candidate "rc" for the DEV image is built
# - A qualification process (e.g., running all tests) is performed on the "rc"
#   image (typically through GitHub actions)
# - If qualification is passed, it becomes "latest".
docker_build_image.rc:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	time \
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IMAGE_RC) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f devops/docker_build/dev.Dockerfile \
		.
	docker image ls $(IMAGE_RC)

docker_build_image_with_cache.rc:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		-t $(IMAGE_RC) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f devops/docker_build/dev.Dockerfile \
		.
	docker image ls $(IMAGE_RC)

# Push the "rc" image to the registry.
docker_push_image.rc:
	docker push $(IMAGE_RC)
	docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)

# Mark the "rc" image as "latest".
docker_tag_rc_image.latest:
	docker tag $(IMAGE_RC) $(ECR_REPO_BASE_PATH):latest

# Push the "latest" image to the registry.
docker_push_image.latest:
	docker push $(ECR_REPO_BASE_PATH):latest

docker_release.latest:
	make docker_build_image_with_cache.rc
	make run_fast_tests.rc
	#make run_slow_tests.rc
	make docker_tag_rc_image.latest
	make docker_push_image.latest
	@echo "==> SUCCESS <=="

# PROD image flow:
# - PROD image has no release candidate
# - The DEV image is qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image becomes "prod".
docker_build_image.prod:
ifdef IMAGE_PROD
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IMAGE_PROD) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f devops/docker_build/prod.Dockerfile \
		.
	docker image ls $(IMAGE_PROD)
else
	@echo "IMAGE_PROD is not defined"
endif

# Push the "prod" image to the registry.
docker_push_image.prod:
ifdef IMAGE_PROD
	docker push $(IMAGE_PROD)
	docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)
else
	@echo "IMAGE_PROD is not defined"
endif

docker_release.prod:
	make docker_build_image_with_cache.rc
	make run_fast_tests.rc
	make run_slow_tests.rc
	make docker_tag_rc_image.latest
	make docker_build_image.prod
	make docker_push_image.prod
	@echo "==> SUCCESS <=="

docker_release.all:
	make docker_release.latest
	make docker_release.prod
	@echo "==> SUCCESS <=="

# #############################################################################
# Git.
# #############################################################################

# Pull all the repos.
git_pull:
	git pull --autostash
	git submodule foreach 'git pull --autostash'

# Clean all the repos.
# TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
# mistake.
git_clean:
	git clean -fd
	git submodule foreach 'git clean -fd'
	find . | \
		grep -E "(tmp.joblib.unittest.cache|.pytest_cache|.mypy_cache|.ipynb_checkpoints|__pycache__|\.pyc|\.pyo$$)" | \
		xargs rm -rf

git_for:
	$(CMD)
	git submodule foreach '$(CMD)'

# #############################################################################
# Linter.
# #############################################################################

lint_branch:
	bash pre-commit.sh run --files $(shell git diff --name-only master...) 2>&1 | tee linter_warnings.txt

# #############################################################################
# Self test.
# #############################################################################

# Run sanity checks on the current build system to make sure it works after
# changes.
#
# NOTE: We need to run with IMAGE_RC since that's what we should be working
# with, when changing the build system.

docker_jupyter_test:
ifeq ($(NO_JUPYTER), 'True')
	@echo "Jupyter is not supported"
else
	J_PORT=19999 \
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		-f devops/compose/docker-compose-jupyter.yml \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server_test
endif

fast_self_tests:
	make print_setup
	make print_debug_setup
	make targets
	make makefiles
	make docker_login
	make docker_repo_images
	make docker_ps
	make docker_pull
	make docker_jupyter_test
	make docker_cmd.rc CMD="pytest --collect-only"
	@echo "==> SUCCESS <=="

slow_self_tests:
	make docker_build_image_with_cache.rc
	make run_blank_tests.rc
	make run_fast_tests.rc
	make docker_build_image.prod
	make run_slow_tests.rc
	@echo "==> SUCCESS <=="

self_tests:
	make fast_self_tests
	make slow_self_tests
	@echo "==> SUCCESS <=="
