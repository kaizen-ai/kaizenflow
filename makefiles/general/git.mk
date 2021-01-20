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
