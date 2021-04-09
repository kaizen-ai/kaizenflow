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
	find . -name "*.mk" -o -name "Makefile" | xargs -n 1 perl -ne 'if (/^\S+:$$/) { print $$_ }' | sort

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

docker_kill_last:
	docker ps -l
	docker rm -f $(shell docker ps -l -q)

docker_kill_all:
	docker ps -a
	docker rm -f $(shell docker ps -a -q)

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
