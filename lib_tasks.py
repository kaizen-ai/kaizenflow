import functools
import logging
import os
import re

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as hsyste

from typing import Any, Dict

_LOG = logging.getLogger(__name__)


# This is used to inject the default params.
_DEFAULT_PARAMS = {}


def set_default_params(params: Dict[str, Any]) -> None:
    global _DEFAULT_PARAMS
    _DEFAULT_PARAMS = params


def get_default_value(key: str) -> Any:
    dbg.dassert_in(key, _DEFAULT_PARAMS)
    dbg.dassert_isinstance(key, str)
    return _DEFAULT_PARAMS[key]


# #############################################################################
# Set-up.
# #############################################################################

@task
def print_setup(ctx):
    _ = ctx
    var_names = "ECR_BASE_PATH ECR_REPO_BASE_PATH".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_value(v)))


# #############################################################################
# Git.
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


# #############################################################################
# Docker.
# #############################################################################


@task
def docker_images_ls_repo(ctx):
    """
    List images in the logged in repo.
    """
    docker_login()
    ecr_base_path = get_default_value("ECR_BASE_PATH")
    ctx.run(f"docker image ls {ecr_base_path}")


@task
def docker_ps(ctx):
    """
    List all running containers.

    ```
    > docker_ps
    CONTAINER ID  user  IMAGE                    COMMAND                    CREATED        STATUS        PORTS  service
    2ece37303ec9  gp    083233266530....:latest  "./docker_build/entry.sh"  5 seconds ago  Up 4 seconds         user_space
    ```
    """
    cmd = r"""
    docker ps \
        --format='table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}\t{{.Label "com.docker.compose.service"}}'
    """
    cmd = _remove_spaces(cmd)
    ctx.run(cmd)


@task
def docker_stats(ctx):
    """
    Report container stats, e.g., CPU, RAM.

    ```
    > docker_stats
    CONTAINER ID  NAME                   CPU %  MEM USAGE / LIMIT     MEM %  NET I/O         BLOCK I/O        PIDS
    2ece37303ec9  ..._user_space_run_30  0.00%  15.74MiB / 31.07GiB   0.05%  351kB / 6.27kB  34.2MB / 12.3kB  4
    ```
    """
    # To change the output format you can use the following --format flags:
    # --format='table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
    cmd = "docker stats --no-stream $(IDS)"
    ctx.run(cmd)


@task
def docker_kill_last(ctx):
    """
    Kill the last Docker container started.
    """
    ctx.run("docker ps -l")
    ctx.run("docker rm -f $(docker ps -l -q)")


@task
def docker_kill_all(ctx):
    """
    Kill all the Docker containers.
    """
    ctx.run("docker ps -a")
    ctx.run("docker rm -f $(docker ps -a -q)")


# #############################################################################
# Docker development.
# #############################################################################


# In the following we use functions from `hsyste` instead of `ctx.run()` since
# `lru_cache` would cache `ctx`.


@functools.lru_cache()
def _get_aws_cli_version():
    # > aws --version
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    cmd = "aws --version"
    res = hsyste.system_to_one_line(cmd)[1]
    # Parse the output.
    m = re.match("aws-cli/((\d+).\d+.\d+)\S", res)
    dbg.dassert(m, "Can't parse '%s'", res)
    version = m.group(1)
    _LOG.debug("version=%s", version)
    major_version = m.group(2)
    _LOG.debug("major_version=%s", major_version)
    return major_version


@task
def docker_login(ctx):
    major_version = _get_aws_cli_version()
    if major_version == 1:
        cmd = "eval $(aws ecr get-login --no-include-email --region us-east-2)"
    else:
        ecr_base_path = get_default_value("ECR_BASE_PATH")
        cmd = ("docker login -u AWS -p $(aws ecr get-login --region us-east-2) " +
               f"https://{ecr_base_path}")
    ctx.run(cmd)


def _get_amp_docker_compose_path() -> str:
    path = git.get_path_from_supermodule()
    if path != "":
        docker_compose_path = "docker-compose-user-space-git-subrepo.yml"
    else:
        docker_compose_path = "docker-compose-user-space.yml"
    # Add the path.
    dir_name = "devops/compose"
    docker_compose_path = os.path.join(dir_name, docker_compose_path)
    docker_compose_path = os.path.abspath(docker_compose_path)
    return docker_compose_path


def _remove_spaces(cmd: str) -> str:
    cmd = cmd.rstrip().lstrip()
    cmd = " ".join(cmd.split())
    return cmd


# TODO(gp): Pass through command line.
use_one_line_cmd = False


def _get_image(stage:str, base_image:str) -> str:
    if stage == "dev":
        suffix = "latest"
    elif stage == "rc":
        suffix = "rc"
    elif stage == "local":
        suffix = "local"
    elif stage == "prod":
        suffix = "prod"
    else:
        raise ValueError("Invalid stage='%s'" % stage)
    image = base_image + ":" + suffix
    return image


def _docker_cmd(ctx, stage: str, base_image: str, docker_compose: str, docker_cmd: str) -> None:
    image = _get_image(stage, base_image)
    # devops/compose/docker-compose-user-space.yml
    dbg.dassert_exists(docker_compose)
    user_name = hsyste.get_user_name()
    cmd = rf"""IMAGE={image} \
    docker-compose \
        -f {docker_compose} \
        run \
        --rm \
        -l user={user_name} \
        user_space \
        {docker_cmd}"""
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


@task
def docker_bash(ctx, stage="local"):
    """
    Start a bash shell inside the container corresponding to a stage.
    """
    base_image = get_default_value("ECR_BASE_PATH")
    docker_compose = _get_amp_docker_compose_path()
    cmd = "bash"
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_cmd(ctx, stage="local", cmd=""):
    """
    Execute the command `cmd` inside a container corresponding to a stage.
    """
    dbg.dassert_ne(cmd, "")
    base_image = get_default_value("ECR_BASE_PATH")
    docker_compose = _get_amp_docker_compose_path()
    # TODO(gp): Do we need to overwrite the entrypoint?
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_jupyter(ctx, stage, port=9999):
    """
    Run jupyter notebook server.
    """
    base_image = get_default_value("ECR_BASE_PATH")
    image = _get_image(stage, base_image)
    # devops/compose/docker-compose-user-space.yml
    docker_compose = _get_amp_docker_compose_path()
    dbg.dassert_exists(docker_compose)
    #
    docker_compose_jupyter = "devops/compose/docker-compose-jupyter.yml"
    docker_compose_jupyter = os.path.abspath(docker_compose_jupyter)
    dbg.dassert_exists(docker_compose_jupyter)
    #
    user_name = hsyste.get_user_name()
    # TODO(gp): Not sure about the order of the -f files.
    cmd = rf"""IMAGE={image} \
    PORT={port} \
    docker-compose \
        -f {docker_compose_jupyter} \
        -f {docker_compose} \
        run \
        --rm \
        -l user={user_name} \
        --service-ports \
        jupyter_server"""
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


# # #############################################################################
# # Images workflows.
# # #############################################################################
#
# ifdef GITHUB_SHA
# IMAGE_RC_SHA:=$(GITHUB_SHA)
# else
# # GITHUB_SHA not found. Setting IMAGE_RC_SHA from HEAD.
# IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
# endif
# IMAGE_RC?=$(IMAGE_RC)


def _get_git_hash() -> str:
    cmd = "git rev-parse HEAD"
    git_hash = hsyste.system_to_one_line(cmd)[1]
    _LOG.debug("git_hash=%s", git_hash)
    return git_hash


def _to_abs_path(filename: str) -> str:
    filename = os.path.abspath(filename)
    dbg.dassert_exists(filename)
    return filename


def _run(ctx: Any, cmd: str) -> None:
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


# Use Docker buildkit or not.
# DOCKER_BUILDKIT = 1
DOCKER_BUILDKIT = 0


# DEV image flow:
# - A release candidate "rc" for the DEV image is built
# - A qualification process (e.g., running all tests) is performed on the "rc"
#   image (typically through GitHub actions)
# - If qualification is passed, it becomes "latest".


@task
def docker_build_image_rc(ctx):
    base_image = get_default_value("ECR_BASE_PATH")
    #
    stage = "rc"
    image_rc = _get_image(stage, base_image)
    #
    image_hash = base_image + ":" + _get_git_hash()
    #
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        --no-cache \
        -t {image_rc} \
        -t {image_hash} \
        -f {dockerfile} \
        .
    """
    _run(ctx, cmd)
# docker image ls $(IMAGE_RC)
#
# docker_build_image_with_cache.rc:
# DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
#     docker build \
#            --progress=plain \
#                       -t $(IMAGE_RC) \
#                           -t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
#                                                     -f devops/docker_build/dev.Dockerfile \
#     .
# docker image ls $(IMAGE_RC)
#
# # Push the "rc" image to the registry.
# docker_push_image.rc:
# docker push $(IMAGE_RC)
# docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)
#
# # Mark the "rc" image as "latest".
# docker_tag_rc_image.latest:
# docker tag $(IMAGE_RC) $(ECR_REPO_BASE_PATH):latest
#
# # Push the "latest" image to the registry.
# docker_push_image.latest:
# docker push $(ECR_REPO_BASE_PATH):latest
#
# docker_release.latest:
# make docker_build_image_with_cache.rc
# make run_fast_tests.rc
# make run_slow_tests.rc
# make docker_tag_rc_image.latest
# make docker_push_image.latest
# @echo "==> SUCCESS <=="
#
# # PROD image flow:
# # - PROD image has no release candidate
# # - The DEV image is qualified
# # - The PROD image is created from the DEV image by copying the code inside the
# #   image
# # - The PROD image becomes "prod".
# docker_build_image.prod:
# ifdef IMAGE_PROD
# DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
#     docker build \
#            --progress=plain \
#                       --no-cache \
#                       -t $(IMAGE_PROD) \
#                           -t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
#                                                     -f devops/docker_build/prod.Dockerfile \
#     .
# docker image ls $(IMAGE_PROD)
# else
# @echo "IMAGE_PROD is not defined"
# endif
#
# # Push the "prod" image to the registry.
# docker_push_image.prod:
# ifdef IMAGE_PROD
# docker push $(IMAGE_PROD)
# docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)
# else
# @echo "IMAGE_PROD is not defined"
# endif
#
# docker_release.prod:
# make docker_build_image_with_cache.rc
# make run_fast_tests.rc
# make run_slow_tests.rc
# make docker_tag_rc_image.latest
# make docker_build_image.prod
# make docker_push_image.prod
# @echo "==> SUCCESS <=="
#
# docker_release.all:
# make docker_release.latest
# make docker_release.prod
# @echo "==> SUCCESS <=="


# #############################################################################
# Linter.
# #############################################################################


@task
def lint_branch(ctx):
    cmd = "git diff --name-only master..."
    files = hsyste.system_to_string(cmd)[1]
    _LOG.info("Files to lint:\n%s", "\n".join(files))
    files = " ".join(files)
    cmd = f"pre-commit.sh run --files $({cmd}) 2>&1 | tee linter_warnings.txt"
    ctx.run(cmd)
