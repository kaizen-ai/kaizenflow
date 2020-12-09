DEV_TOOLS_PROD_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod
PARTICLE_ENV_IMAGE?=083233266530.dkr.ecr.us-east-2.amazonaws.com/particle_env:latest
IMAGE?=$(PARTICLE_ENV_IMAGE)
# #############################################################################
# Docker.
# #############################################################################

# Log in to AWS ECR.
docker_login:
	eval `aws ecr get-login --no-include-email --region us-east-2`

# Log in to AWS ECR (if your `awscli` version higher or equal `2`).
docker_login_v2:
	docker login -u AWS -p $(aws ecr get-login --region us-east-2) https://083233266530.dkr.ecr.us-east-2.amazonaws.com

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
