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
make_print_targets:
	find . -name "*.mk" -o -name "Makefile" | xargs -n 1 perl -ne 'if (/^\S+:$$/) { print $$_ }'

# Print all the makefiles.
make_print_makefiles:
	find . -name "*.mk" -o -name "Makefile" | sort

# List images in the logged in repo.
docker_repo_images:
	docker image ls $(ECR_BASE_PATH)

# List all running containers:
#   ```
#   > docker_ps
#   CONTAINER ID  user  IMAGE                                COMMAND                 CREATED        STATUS        PORTS  service
#   2ece37303ec9  gad   083233266530....particle_env:latest  "./docker_build/entr…"  5 seconds ago  Up 4 seconds         user_space
#   ```
docker_ps:
	docker ps --format='table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}\t{{.Label "com.docker.compose.service"}}'

# Report container stats, e.g., CPU, RAM.
#   ```
#   > docker_stats
#   CONTAINER ID  NAME                                  CPU %  MEM USAGE / LIMIT     MEM %  NET I/O         BLOCK I/O        PIDS
#   2ece37303ec9  commodity_research_user_space_run_30  0.00%  15.74MiB / 31.07GiB   0.05%  351kB / 6.27kB  34.2MB / 12.3kB  4
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


# Run bash inside container with activated environment.
docker_bash:
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		user_space \
		bash

# Run the script inside the container with activated environment.
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

# Run jupyter notebook server.
# E.g., run on port 10000 using a specific image:
# > make docker_jupyter J_PORT=10000 IMAGE="083233266530.dkr.ecr.us-east-2.amazonaws.com/amp_env:rc"
J_PORT?=9999
docker_jupyter:
ifndef NO_JUPYTER
	J_PORT=$(J_PORT) \
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose-jupyter.yml \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server
else
	@echo "Jupyter is not supported"
endif

# #############################################################################
# Run tests with "latest" image.
# #############################################################################

print_debug_setup:
	@echo "SUBMODULE_NAME=$(SUBMODULE_NAME)"
	@echo "DOCKER_COMPOSE_USER_SPACE=${DOCKER_COMPOSE_USER_SPACE}"
	@echo "NO_JUPYTER=$(NO_JUPYTER)"
	@echo "NO_FAST_TESTS=$(NO_FAST_TESTS)"
	@echo "NO_SLOW_TESTS=$(NO_SLOW_TESTS)"
	@echo "NO_SUPERSLOW_TESTS=$(NO_SUPERSLOW_TESTS)"

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
ifndef NO_FAST_TESTS
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests
else
	echo "No fast tests"
endif

run_slow_tests:
ifndef NO_SLOW_TESTS
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests
else
	echo "No slow tests"
endif

run_superslow_tests:
ifndef NO_SUPERSLOW_TESTS
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests
else
	echo "No superslow tests"
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
ifndef NO_FAST_TESTS
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests
else
	echo "No fast tests"
endif

run_slow_tests.rc:
ifndef NO_SLOW_TESTS
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests
else
	echo "No slow tests"
endif

run_superslow_tests.rc:
ifndef NO_SUPERSLOW_TESTS
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests
else
	echo "No superslow tests"
endif

# #############################################################################
# GH actions tests for "latest" image.
# #############################################################################

# For the GH actions we assume that if we call the target, it must work: thus
# we don't use NO_{FAST,SLOW,SUPERSLOW}_TESTS.

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
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests.gh_action

run_slow_tests.gh_action:
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests.gh_action

run_superslow_tests.gh_action:
	_IMAGE=$(IMAGE_DEV) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests.gh_action

# #############################################################################
# GH actions tests for "rc" image.
# #############################################################################

# Test using release candidate image via GH Actions.

run_fast_tests.gh_action_rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
	make _run_tests.gh_action

run_slow_tests.gh_action_rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
	make _run_tests.gh_action

run_superslow_tests.gh_action_rc:
	_IMAGE=$(IMAGE_RC) \
	_CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
	make _run_tests.gh_action

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
docker_build_rc_image:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	time \
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IMAGE_RC) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f devops/docker_build/dev.Dockerfile \
		.

docker_build_rc_image_with_cache:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		-t $(IMAGE_RC) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f devops/docker_build/dev.Dockerfile \
		.

# Push the "rc" image to the registry.
docker_push_rc_image:
	docker push $(IMAGE_RC)
	docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)

# Make the "rc" image as "latest".
docker_tag_rc_image_latest:
	docker tag $(IMAGE_RC) $(ECR_REPO_BASE_PATH):latest

# Push the "latest" image to the registry.
docker_push_latest_image:
	docker push $(ECR_REPO_BASE_PATH):latest

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
else
	@echo "IMAGE_PROD is not defined: nothing to do"
endif

# Push the "prod" image to the registry.
docker_push_image.prod:
ifdef IMAGE_PROD
	docker push $(IMAGE_PROD)
	docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)
else
	@echo "IMAGE_PROD is not defined: nothing to do"
endif

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

git_for:
	$(CMD)
	git submodule foreach '$(CMD)'

# #############################################################################
# Linter.
# #############################################################################

lint_branch:
	bash pre-commit.sh run --files $(shell git diff --name-only master...)

# #############################################################################
# Pre-commit installation.
# #############################################################################

# Install pre-commit shell script.
precommit_install:
	docker run \
		--rm -t \
		-v "$(shell pwd)":/src \
		--workdir /src \
		--entrypoint="bash" \
		$(DEV_TOOLS_PROD_IMAGE) \
		/dev_tools/pre_commit_scripts/install_precommit_script.sh

# Uninstall pre-commit shell script.
precommit_uninstall:
	docker run \
		--rm -t \
		-v "$(shell pwd)":/src \
		--workdir /src \
		--entrypoint="bash" \
		$(DEV_TOOLS_PROD_IMAGE) \
		/dev_tools/pre_commit_scripts/uninstall_precommit_script.sh

# Install pre-commit git-hook.
precommit_install_githooks:
	docker run \
		--rm -t \
		-v "$(shell pwd)":/src \
		--workdir /src \
		--entrypoint="bash" \
		$(DEV_TOOLS_PROD_IMAGE) \
		/dev_tools/pre_commit_scripts/install_precommit_hook.sh

# Uninstall pre-commit hook.
precommit_uninstall_githooks:
	docker run \
		--rm -t \
		-v "$(shell pwd)":/src \
		--workdir /src \
		--entrypoint="bash" \
		$(DEV_TOOLS_PROD_IMAGE) \
		/dev_tools/pre_commit_scripts/uninstall_precommit_hook.sh

# #############################################################################
# Self test.
# #############################################################################

# Run sanity checks on the current build system to make sure it works after
# changes.
#
# NOTE: We need to run with IMAGE_RC since that's what we should be working
# with, when changing the build system.

docker_jupyter_test:
ifndef NO_JUPYTER
	J_PORT=19999 \
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose-jupyter.yml \
		-f $(DOCKER_COMPOSE_USER_SPACE) \
		run \
		--rm \
		-l user=$(USER) \
		--service-ports \
		jupyter_server_test
else
	@echo "Jupyter is not supported"
endif


fast_self_tests:
	make print_setup
	make print_debug_setup
	make make_print_targets
	make make_print_makefiles
	make docker_login
	make docker_repo_images
	make docker_ps
	make docker_pull
	make docker_cmd CMD="echo" IMAGE=$(IMAGE_RC)
	make docker_jupyter_test

slow_self_tests:
	make docker_build_rc_image_with_cache
	make run_blank_tests.rc
	make run_fast_tests.rc
	make docker_build_image.prod
	make run_slow_tests.rc

self_tests:
	make fast_self_tests
	make slow_self_tests
