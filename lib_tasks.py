import functools
import logging
import os
import re
from typing import Any, Dict, Match, Optional

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)


# This is used to inject the default params.
_DEFAULT_PARAMS = {}


# NOTE: We need to use a `# type: ignore` for all the @task functions because
# invoke infers argument type from the code and mypy annotations confuse it
# (see https://github.com/pyinvoke/invoke/issues/357)


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
def print_setup(ctx):  # type: ignore
    _ = ctx
    var_names = "ECR_BASE_PATH ECR_REPO_BASE_PATH".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_value(v)))


@task
def activate_poetry(ctx):  # type: ignore
    """
    Print how to activate the virtual environment.
    """
    cmd = '''cd devops/docker_build; \
            FILE="$(poetry env info --path)/bin/activate"; \
            echo "source $FILE"'''
    ctx.run(cmd)


# #############################################################################
# Git.
# #############################################################################

# # Pull all the needed images from the registry.
# docker_pull:
# docker pull $(IMAGE_DEV)
# docker pull $(DEV_TOOLS_IMAGE_PROD)


@task
def git_pull(ctx):  # type: ignore
    """
    Pull all the repos.
    """
    cmd = "git pull --autostash"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git pull --autostash'"
    ctx.run(cmd)


@task
def git_pull_master(ctx):  # type: ignore
    """
    Pull master without changing branch.
    """
    cmd = "git fetch origin master:master"
    ctx.run(cmd)


@task
def git_clean(ctx):  # type: ignore
    """
    Clean all the repos.
    """
    # TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
    #  mistake.
    cmd = "git clean -fd"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git clean -fd'"
    ctx.run(cmd)
    # pylint: disable=line-too-long
    cmd = r"""find . | \
    grep -E "(tmp.joblib.unittest.cache|.pytest_cache|.mypy_cache|.ipynb_checkpoints|__pycache__|\.pyc|\.pyo$$)" | \
    xargs rm -rf"""
    # pylint: enable=line-too-long
    ctx.run(cmd)


# #############################################################################
# Docker.
# #############################################################################


@task
def docker_images_ls_repo(ctx):  # type: ignore
    """
    List images in the logged in repo.
    """
    docker_login(ctx)
    ecr_base_path = get_default_value("ECR_BASE_PATH")
    ctx.run(f"docker image ls {ecr_base_path}")


@task
def docker_ps(ctx):  # type: ignore
    # pylint: disable=line-too-long
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
    # pylint: enable=line-too-long
    cmd = _remove_spaces(cmd)
    ctx.run(cmd)


@task
def docker_stats(ctx):  # type: ignore
    # pylint: disable=line-too-long
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
    # pylint: enable=line-too-long
    cmd = "docker stats --no-stream $(IDS)"
    ctx.run(cmd)


@task
def docker_kill_last(ctx):  # type: ignore
    """
    Kill the last Docker container started.
    """
    ctx.run("docker ps -l")
    ctx.run("docker rm -f $(docker ps -l -q)")


@task
def docker_kill_all(ctx):  # type: ignore
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
def _get_aws_cli_version() -> int:
    # > aws --version
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    cmd = "aws --version"
    res = hsyste.system_to_one_line(cmd)[1]
    # Parse the output.
    m = re.match(r"aws-cli/((\d+).\d+.\d+)\S", res)
    dbg.dassert(m, "Can't parse '%s'", res)
    m: Match[Any]
    version = m.group(1)
    _LOG.debug("version=%s", version)
    major_version = int(m.group(2))
    _LOG.debug("major_version=%s", major_version)
    return major_version


@task
def docker_login(ctx):  # type: ignore
    major_version = _get_aws_cli_version()
    # TODO(gp): We should get this programmatically from ~/aws/.credentials
    region = "us-east-1"
    if major_version == 1:
        cmd = f"eval $(aws ecr get-login --no-include-email --region {region})"
    else:
        ecr_base_path = get_default_value("ECR_BASE_PATH")
        cmd = (
            f"docker login -u AWS -p $(aws ecr get-login --region {region}) "
            + f"https://{ecr_base_path}"
        )
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


def _get_image(stage: str, base_image: Optional[str]=None) -> str:
    """
    665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local
    """
    # Docker refers the default image as "latest", although in our stage
    # nomenclature we call it "dev".
    dbg.dassert_in(stage, "local dev prod".split())
    if base_image is None:
        # 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
        base_image = get_default_value("ECR_BASE_PATH") + "/" + get_default_value("BASE_IMAGE")
    image = base_image + ":" + stage
    return image


def _docker_cmd(
    ctx: Any, stage: str, base_image: str, docker_compose: str, cmd: str
) -> None:
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
        {cmd}"""
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


@task
def docker_bash(ctx, stage="local"):  # type: ignore
    """
    Start a bash shell inside the container corresponding to a stage.
    """
    base_image = get_default_value("ECR_BASE_PATH")
    docker_compose = _get_amp_docker_compose_path()
    cmd = "bash"
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_cmd(ctx, stage="local", cmd=""):  # type: ignore
    """
    Execute the command `cmd` inside a container corresponding to a stage.
    """
    dbg.dassert_ne(cmd, "")
    base_image = get_default_value("ECR_BASE_PATH")
    docker_compose = _get_amp_docker_compose_path()
    # TODO(gp): Do we need to overwrite the entrypoint?
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_jupyter(ctx, stage, port=9999):  # type: ignore
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
        -f {docker_compose} \
        -f {docker_compose_jupyter} \
        run \
        --rm \
        -l user={user_name} \
        --service-ports \
        jupyter_server"""
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


# #############################################################################
# Images workflows.
# #############################################################################


def _get_git_hash() -> str:
    cmd = "git rev-parse HEAD"
    git_hash: str = hsyste.system_to_one_line(cmd)[1]
    _LOG.debug("git_hash=%s", git_hash)
    return git_hash


# TODO(gp): Fold this in _get_image("hash")
def _get_image_githash() -> str:
    base_image: str = get_default_value("ECR_BASE_PATH")
    image_hash = base_image + ":" + _get_git_hash()
    return image_hash


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
# - A "local" image which is a release candidate for the DEV image is built
# - A qualification process (e.g., running all tests) is performed on the "local"
#   image (typically through GitHub actions)
# - If qualification is passed, it becomes "latest".


@task
def docker_build_local_image(ctx, cache=True):  # type: ignore
    """
    Build a local as a release candidate image.
    """
    image_local = _get_image("local")
    image_hash = _get_image_githash()
    #
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no_cache" if not cache else ""
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        -t {image_local} \
        -t {image_hash} \
        -f {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


# @task
# def docker_push_image(ctx, stage):  # type: ignore
#     """
#     Push an image for the given `stage` to ECR.
#     """
#     base_image = get_default_value("ECR_BASE_PATH")
#     image_local = _get_image(stage, base_image)
#     #
#     image_hash = _get_image_githash()
#     cmd = f"docker push {image_local}"
#     _run(ctx, cmd)
#     cmd = f"docker push {image_hash}"
#     _run(ctx, cmd)


@task
def docker_push_local_image_to_dev(ctx):  # type: ignore
    """
    Mark the "local" image as "dev" and "latest" and push to ECR.
    """
    image_local = _get_image("local")
    #image_hash = _get_image_githash()
    #
    docker_login(ctx)
    #
    cmd = f"docker push {image_local}"
    _run(ctx, cmd)
    #
    image_dev = _get_image("dev")
    cmd = f"docker tag {image_local} {image_dev}"
    _run(ctx, cmd)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd)
    #
    image_latest = _get_image("latest", base_image)
    cmd = f"docker tag {image_local} {image_latest}"
    _run(ctx, cmd)
    cmd = f"docker push {image_latest}"
    _run(ctx, cmd)


# @task
# def docker_push_image_latest(ctx):  # type: ignore
#     """
#     Push the "latest" image to the registry.
#     """
#     cmd = f"docker push {ecr_repo_base_path}:latest"
#     _run(ctx, cmd)


@task
def docker_release_dev_image(ctx):  # type: ignore
    """
    Build, test, and release to ECR the latest image.
    """
    #docker_build_image_local(ctx, cache=True)
    #run_fast_tests(ctx, stage="local")
    #run_slow_tests(ctx, stage="local")
    docker_push_local_image_to_dev(ctx)
    _LOG.info("==> SUCCESS <==")


# PROD image flow:
# - PROD image has no release candidate
# - The DEV image is qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image becomes "prod".


# TODO(gp): Remove redundancy with docker_build_local_image().
@task
def docker_build_image_prod(ctx, cache=False):  # type: ignore
    """
    Build a prod image.
    """
    stage = "prod"
    image_local = _get_image(stage)
    #
    image_hash = _get_image_githash()
    #
    dockerfile = "devops/docker_build/prod.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no_cache" if not cache else ""
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        -t {image_local} \
        -t {image_hash} \
        -f {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


@task
def docker_release_image_prod(ctx, cache=False):  # type: ignore
    """
    Build, test, and release to ECR the prod image.
    """
    # TODO(gp): Factor this out and reuse.
    docker_build_local_image(ctx, cache=cache)
    run_fast_tests(ctx, stage="local")
    run_slow_tests(ctx, stage="local")
    docker_tag_local_image_as_dev(ctx)
    #
    docker_build_image_prod(ctx, cache=cache)
    docker_push_image(ctx, stage="prod")
    _LOG.info("==> SUCCESS <==")


# docker_release.all:
# make docker_release.latest
# make docker_release.prod
# @echo "==> SUCCESS <=="

# # #############################################################################
# # Run tests.
# # #############################################################################

def _run_tests(ctx, stage, cmd):
    base_image = get_default_value("ECR_BASE_PATH")
    docker_compose = _get_amp_docker_compose_path()
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def run_blank_tests(ctx, stage="dev"):
    cmd = "(pytest -h >/dev/null)"
    _run_tests(ctx, stage, cmd)


@task
def run_fast_tests(ctx, stage="dev", pytest_opts=""):
    run_tests_dir = "devops/docker_scripts"
    cmd = f"{run_tests_dir}/run_fast_tests.sh {pytest_opts}"
    _run_tests(ctx, stage, cmd)


@task
def run_slow_tests(ctx, stage="dev", pytest_opts=""):
    run_tests_dir = "devops/docker_scripts"
    cmd = f"{run_tests_dir}/run_slow_tests.sh {pytest_opts}"
    _run_tests(ctx, stage, cmd)


@task
def run_superslow_tests(ctx, stage="dev", pytest_opts=""):
    run_tests_dir = "devops/docker_scripts"
    cmd = f"{run_tests_dir}/run_superslow_tests.sh {pytest_opts}"
    _run_tests(ctx, stage, cmd)


# # #############################################################################
# # GH actions tests for "latest" image.
# # #############################################################################
#
# _run_tests.gh_action:
# IMAGE=$(_IMAGE) \
#     docker-compose \
#     -f devops/compose/docker-compose.yml \
#        -f devops/compose/docker-compose.gh_actions.yml \
#     run \
#     --rm \
#     -l user=$(USER) \
#     app \
#     $(_CMD)
#
# run_fast_tests.gh_action:
# ifeq ($(NO_FAST_TESTS), 'True')
# @echo "No fast tests"
# else
# _IMAGE=$(IMAGE_DEV) \
#     _CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# run_slow_tests.gh_action:
# ifeq ($(NO_SLOW_TESTS), 'True')
# @echo "No slow tests"
# else
# _IMAGE=$(IMAGE_DEV) \
#     _CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# run_superslow_tests.gh_action:
# ifeq ($(NO_SUPERSLOW_TESTS), 'True')
# @echo "No superslow tests"
# else
# _IMAGE=$(IMAGE_DEV) \
#     _CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# # #############################################################################
# # GH actions tests for "local" image.
# # #############################################################################
#
# # Test using release candidate image via GH Actions.
#
# run_fast_tests.gh_action_rc:
# ifeq ($(NO_FAST_TESTS), 'True')
# @echo "No fast tests"
# else
# _IMAGE=$(IMAGE_RC) \
#     _CMD="$(RUN_TESTS_DIR)/run_fast_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# run_slow_tests.gh_action_rc:
# ifeq ($(NO_SLOW_TESTS), 'True')
# @echo "No slow tests"
# else
# _IMAGE=$(IMAGE_RC) \
#     _CMD="$(RUN_TESTS_DIR)/run_slow_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# run_superslow_tests.gh_action_rc:
# ifeq ($(NO_SUPERSLOW_TESTS), 'True')
# @echo "No superslow tests"
# else
# _IMAGE=$(IMAGE_RC) \
#     _CMD="$(RUN_TESTS_DIR)/run_superslow_tests.sh" \
#     make _run_tests.gh_action
# endif
#
# docker_bash.gh_action_rc:
# IMAGE=$(IMAGE_RC) \
#     docker-compose \
#     -f devops/compose/docker-compose.yml \
#        -f devops/compose/docker-compose.gh_actions.yml \
#     run \
#     --rm \
#     -l user=$(USER) \
#     app \
#     bash

# #############################################################################
# Linter.
# #############################################################################


@task
def lint_docker_pull(ctx):  # type: ignore
    ecr_base_path = "083233266530.dkr.ecr.us-east-2.amazonaws.com"
    dev_tools_image_prod = f"{ecr_base_path}/dev_tools:prod"
    docker_login(ctx)
    cmd = f"docker pull {dev_tools_image_prod}"
    ctx.run(cmd, pty=True)


# TODO(gp): Pass pre-commit phases.
@task
def lint_branch(ctx):  # type: ignore
    cmd = "git diff --name-only master..."
    files = hsyste.system_to_string(cmd)[1]
    _LOG.info("Files to lint:\n%s", "\n".join(files))
    cmd = f"pre-commit.sh run --files $({cmd}) 2>&1 | tee linter_warnings.txt"
    ctx.run(cmd)