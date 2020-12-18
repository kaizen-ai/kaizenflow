DEV_TOOLS_PROD_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod
PARTICLE_ENV_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/particle_env:latest
IMAGE?=$(PARTICLE_ENV_IMAGE)
# #############################################################################
# Docker.
# #############################################################################

# Log in to AWS ECR.
AWSCLI_VERSION=$(shell aws --version | awk '{print $$1}' | awk -F"/" '{print $$2}')
AWSCLI_MAJOR_VERSION=$(shell echo "$(AWSCLI_VERSION)" | awk -F"." '{print $$1}')
docker_login:
	@echo AWS CLI version: $(AWSCLI_VERSION)
	@echo AWS CLI major version: $(AWSCLI_MAJOR_VERSION)
ifeq ($(AWSCLI_MAJOR_VERSION),1)
	eval `aws ecr get-login --no-include-email --region us-east-2`
else
	docker login -u AWS -p $(aws ecr get-login --region us-east-2) https://083233266530.dkr.ecr.us-east-2.amazonaws.com
endif

# Pull an image from the registry.
docker_pull:
	docker pull $(PARTICLE_ENV_IMAGE)
	docker pull $(DEV_TOOLS_PROD_IMAGE)

# #############################################################################
# Pre-commit instalation
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

lint_branch:
	bash pre-commit.sh run --files $(shell git diff --name-only origin/master)

# #############################################################################
# Tests
# #############################################################################
# Run fast tests.
docker_fast:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm fast_tests

# Run slow tests.
docker_slow:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm slow_tests

# Run superslow tests.
docker_superslow:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm superslow_tests

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
# Git.
# #############################################################################

# Pull all the repos.
git_pull:
	git pull --autostash && \
	git submodule foreach 'git pull --autostash'

# Clean all the repos.
# TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
# mistake.
git_clean:
	git clean -fd && \
	git submodule foreach 'git clean -fd'

git_for:
	$(CMD) && \
	git submodule foreach '$(CMD)'
