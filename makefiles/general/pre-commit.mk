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
