import logging
import re

from invoke import task

import helpers.dbg as dbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# #############################################################################
# Setup.
# #############################################################################

ECR_BASE_PATH = "083233266530.dkr.ecr.us-east-2.amazonaws.com"

# TODO(gp): -> amp?
ECR_REPO_BASE_PATH = f"{ECR_BASE_PATH}/amp_env"

# When testing a change to the build system in a branch you can use a different
# image, e.g., `XYZ_tmp` to not interfere with the prod system.
# ECR_REPO_BASE_PATH:=$(ECR_BASE_PATH)/amp_env_tmp

# Target image for the common actions.
IMAGE_DEV = f"{ECR_REPO_BASE_PATH}:latest"
IMAGE_RC = f"{ECR_REPO_BASE_PATH}:rc"

DEV_TOOLS_IMAGE_PROD = f"{ECR_BASE_PATH}/dev_tools:prod"

# Point to the dir with the run_scripts.
RUN_TESTS_DIR = "devops/docker_scripts"

NO_SUPERSLOW_TESTS = True


@task
def print_setup(ctx):
    _ = ctx
    var_names = "ECR_BASE_PATH ECR_REPO_BASE_PATH DEV_TOOLS_IMAGE_PROD IMAGE_DEV IMAGE_RC"
    for v in var_names.split():
        print(hprint.to_str(v))


# #############################################################################
# Docker development.
# #############################################################################

# # Pull all the needed images from the registry.
# docker_pull:
# docker pull $(IMAGE_DEV)
# docker pull $(DEV_TOOLS_IMAGE_PROD)

@task
def git_pull(ctx):
    """
    Pull all the repos.
    """
    cmd = "git pull --autostash"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git pull --autostash'"
    ctx.run(cmd)


@task
def git_pull_master(ctx):
    """
    Pull master without changing branch.
    """
    cmd = "git fetch origin master:master"
    ctx.run(cmd)


@task
def git_clean(ctx):
    """
    Clean all the repos.
    """
    # TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
    #  mistake.
    cmd = "git clean -fd"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git clean -fd'"
    ctx.run(cmd)
    cmd = """find . | \
    grep -E "(tmp.joblib.unittest.cache|.pytest_cache|.mypy_cache|.ipynb_checkpoints|__pycache__|\.pyc|\.pyo$$)" | \
    xargs rm -rf"""
    ctx.run(cmd)


@task
def docker_login(ctx):
    res = ctx.run("aws --version", hide='out')
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    res = res.stdout
    m = re.match("aws-cli/((\d+).\d+.\d+)\S", res)
    dbg.dassert(m, "Can't parse '%s'", res)
    version = m.group(1)
    major_version = m.group(2)
    _LOG.debug("version=%s", version)
    if major_version == 1:
        cmd = "eval `aws ecr get-login --no-include-email --region us-east-2`"
    else:
        cmd = "docker login -u AWS -p $(aws ecr get-login --region us-east-2) https://$(ECR_REPO_BASE_PATH)"
    ctx.run(cmd)
#
# # Print all the makefile targets.
# targets:
# find . -name "*.mk" -o -name "Makefile" | xargs -n 1 perl -ne 'if (/^\S+:$$/) { print $$_ }'
#
# # Print all the makefiles.
# makefiles:
# find . -name "*.mk" -o -name "Makefile" | sort
#
# # List images in the logged in repo.
# docker_repo_images:
# docker image ls $(ECR_BASE_PATH)
#
# # List all running containers:
# #   ```
# #   > docker_ps
# #   CONTAINER ID  user  IMAGE                    COMMAND                 	  CREATED        STATUS        PORTS  service
# #   2ece37303ec9  gad   083233266530....:latest  "./docker_build/entrâ¦"  5 seconds ago  Up 4 seconds         user_space
# #   ```
# docker_ps:
# docker ps --format='table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}\t{{.Label "com.docker.compose.service"}}'
#
# # Report container stats, e.g., CPU, RAM.
# #   ```
# #   > docker_stats
# #   CONTAINER ID  NAME                   CPU %  MEM USAGE / LIMIT     MEM %  NET I/O         BLOCK I/O        PIDS
# #   2ece37303ec9  ..._user_space_run_30  0.00%  15.74MiB / 31.07GiB   0.05%  351kB / 6.27kB  34.2MB / 12.3kB  4
# #   ```
# docker_stats:
# # To change output format you can use following --format flag with `docker stats` command.
# # --format='table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
# docker stats --no-stream $(IDS)



@task
def get_platform(c):
    uname = c.run("uname -s").stdout.strip()
    if uname == 'Darwin':
        return "You paid the Apple tax!"
    elif uname == 'Linux':
        return "Year of Linux on the desktop!"


@task
def replace(c, path, search, replacement):
    # Assume systems have sed, and that some (eg macOS w/ Homebrew) may
    # have gsed, implying regular sed is BSD style.
    has_gsed = c.run("which gsed", warn=True, hide=True)
    # Set command to run accordingly
    binary = "gsed" if has_gsed else "sed"
    #c.run(f"{binary} -e 's/{search}/{replacement}/g' {path}")
    c.run("sed")
