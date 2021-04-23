import functools
import logging
import os
import re
import sys
from typing import Any, Dict, Match

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as hprint
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


if not (("-d" in sys.argv) or ("--debug" in sys.argv)):
    dbg.init_logger(verbosity=logging.INFO)

# #############################################################################
# Set-up.
# #############################################################################


@task
def print_setup(ctx):  # type: ignore
    _LOG.info(">")
    _ = ctx
    var_names = "ECR_BASE_PATH ECR_REPO_BASE_PATH".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_value(v)))


@task
def activate_poetry(ctx):  # type: ignore
    """
    Print how to activate the virtual environment.
    """
    _LOG.info(">")
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
    _LOG.info(">")
    cmd = "git pull --autostash"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git pull --autostash'"
    ctx.run(cmd)


@task
def git_pull_master(ctx):  # type: ignore
    """
    Pull master without changing branch.
    """
    _LOG.info(">")
    cmd = "git fetch origin master:master"
    ctx.run(cmd)


@task
def git_clean(ctx):  # type: ignore
    """
    Clean all the repos.
    """
    _LOG.info(">")
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


@task
def git_diff_master_files(ctx):  # type: ignore
    _LOG.info(">")
    cmd = "git diff --name-only master..."
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
    _LOG.info(">")
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


# TODO(gp): Pass through command line using a global switch.
use_one_line_cmd = False


@functools.lru_cache()
def _get_git_hash() -> str:
    cmd = "git rev-parse HEAD"
    git_hash: str = hsyste.system_to_one_line(cmd)[1]
    _LOG.debug("git_hash=%s", git_hash)
    return git_hash


def _check_image(image: str) -> None:
    """
    An image should look like:

    665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local
    """
    internet_address_re = "^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
    image_re = "([a-z0-9]+(-[a-z0-9]+)*)"
    tag_re = "([a-z0-9]+(-[a-z0-9]+)*)"
    m = re.match(f"^{internet_address_re}\/{image_re}:{tag_re}$", image)
    dbg.dassert(m, "Invalid image: '%s'", image)


def _check_base_image(base_image: str) -> None:
    """
    A base image should look like.

    665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    internet_address_re = "([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
    image_re = "([a-z0-9]+(-[a-z0-9]+)*)"
    regex = f"^{internet_address_re}\/{image_re}$"
    _LOG.debug("regex=%s", regex)
    m = re.match(regex, base_image)
    dbg.dassert(m, "Invalid base_image: '%s'", base_image)


def _get_base_image(base_image: str) -> str:
    """
    :return: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    if base_image == "":
        base_image = (
            get_default_value("ECR_BASE_PATH")
            + "/"
            + get_default_value("BASE_IMAGE")
        )
    _check_base_image(base_image)
    return base_image


def _get_image(stage: str, base_image: str) -> str:
    """
    :param base_image: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    :return: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local
    """
    # Docker refers the default image as "latest", although in our stage
    # nomenclature we call it "dev".
    dbg.dassert_in(stage, "local dev prod hash".split())
    if stage == "hash":
        stage = _get_git_hash()
    # Get the base image.
    base_image = _get_base_image(base_image)
    _check_base_image(base_image)
    # Get the full image.
    image = base_image + ":" + stage
    _check_image(image)
    return image


def _docker_cmd(
    ctx: Any, stage: str, base_image: str, docker_compose: str, cmd: str
) -> None:
    """
    :param base_image: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    :param docker_compose: e.g. devops/compose/docker-compose-user-space.yml
    """
    hprint.log(_LOG, logging.DEBUG, "stage base_image docker_compose cmd")
    image = _get_image(stage, base_image)
    _LOG.debug("base_image=%s stage=%s -> image=%s", base_image, stage, image)
    #
    _check_image(image)
    dbg.dassert_exists(docker_compose)
    #
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
    _LOG.info(">")
    base_image = None
    docker_compose = _get_amp_docker_compose_path()
    cmd = "bash"
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_cmd(ctx, stage="local", cmd=""):  # type: ignore
    """
    Execute the command `cmd` inside a container corresponding to a stage.
    """
    _LOG.info(">")
    dbg.dassert_ne(cmd, "")
    base_image = None
    docker_compose = _get_amp_docker_compose_path()
    # TODO(gp): Do we need to overwrite the entrypoint?
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def docker_jupyter(ctx, stage, port=9999, self_test=False, base_image=""):  # type: ignore
    """
    Run jupyter notebook server.
    """
    _LOG.info(">")
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
    service = "jupyter_server" if self_test else "jupyter_server_test"
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
        {service}"""
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, pty=True)


# #############################################################################
# Images workflows.
# #############################################################################


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
# - A "local" image (which is a release candidate for the DEV image) is built
# - A qualification process (e.g., running all tests) is performed on the "local"
#   image (typically through GitHub actions)
# - If qualification is passed, it becomes "latest".


# For base_image, we use "" as default instead None since pyinvoke can only infer
# a single type.
@task
def docker_build_local_image(ctx, cache=True, base_image=""):  # type: ignore
    """
    Build a local as a release candidate image.
    """
    _LOG.info(">")
    image_local = _get_image("local", base_image)
    image_hash = _get_image("hash", base_image)
    #
    _check_image(image_local)
    _check_image(image_hash)
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


@task
def docker_push_local_image_to_dev(ctx, base_image=""):  # type: ignore
    """
    Mark the "local" image as "dev" and "latest" and push to ECR.
    """
    _LOG.info(">")
    docker_login(ctx)
    #
    image_local = _get_image("local", base_name)
    cmd = f"docker push {image_local}"
    _run(ctx, cmd)
    #
    image_hash = _get_image("hash", base_name)
    cmd = f"docker tag {image_local} {image_hash}"
    _run(ctx, cmd)
    cmd = f"docker push {image_hash}"
    _run(ctx, cmd)
    #
    image_dev = _get_image("dev", base_name)
    cmd = f"docker tag {image_local} {image_dev}"
    _run(ctx, cmd)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd)


@task
def docker_release_dev_image(  # type: ignore
    ctx, cache=True, run_fast=True, run_slow=True, run_superslow=False
):
    """
    Build, test, and release to ECR the latest "dev" image.
    """
    _LOG.info(">")
    # Build image.
    docker_build_local_image(ctx, cache=cache)
    # Run tests.
    stage = "local"
    if run_fast:
        run_fast_tests(ctx, stage=stage)
    if run_slow:
        run_slow_tests(ctx, stage=stage)
    if run_superslow:
        run_superslow_tests(ctx, stage=stage)
    # Push.
    docker_push_local_image_to_dev(ctx)
    _LOG.info("==> SUCCESS <==")


# PROD image flow:
# - PROD image has no release candidate
# - The DEV image is qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image is tagged as "prod"


# TODO(gp): Remove redundancy with docker_build_local_image().
@task
def docker_build_image_prod(ctx, cache=False, base_image=""):  # type: ignore
    """
    Build a prod image.
    """
    _LOG.info(">")
    image_prod = _get_image("prod", base_name)
    #
    _check_image(image_prod)
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
        -t {image_prod} \
        -f {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_prod}"
    _run(ctx, cmd)


@task
def docker_release_prod_image(  # type: ignore
    ctx,
    cache=False,
    run_fast=True,
    run_slow=True,
    run_superslow=False,
    base_image="",
):
    """
    Build, test, and release to ECR the prod image.
    """
    _LOG.info(">")
    # Build dev image.
    docker_build_local_image(ctx, cache=cache)
    docker_push_local_image_to_dev(ctx)
    # Build prod image.
    docker_build_image_prod(ctx, cache=cache)
    # Run tests.
    stage = "prod"
    if run_fast:
        run_fast_tests(ctx, stage=stage)
    if run_slow:
        run_slow_tests(ctx, stage=stage)
    if run_superslow:
        run_superslow_tests(ctx, stage=stage)
    # Push prod image.
    image_prod = _get_image("prod", base_image)
    cmd = f"docker push {image_prod}"
    _run(ctx, cmd)
    _LOG.info("==> SUCCESS <==")


@task
def docker_release_all(ctx):  # type: ignore
    """
    Release to ECT both dev and prod image.
    """
    docker_release_dev_image(ctx)
    docker_release_prod_image(ctx)
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# Run tests.
# #############################################################################


def _run_tests(ctx: Any, stage: str, cmd: str) -> None:
    """
    Run a command in the set-up to run tests.
    """
    base_image = None
    docker_compose = _get_amp_docker_compose_path()
    _docker_cmd(ctx, stage, base_image, docker_compose, cmd)


@task
def run_blank_tests(ctx, stage="dev"):  # type: ignore
    _LOG.info(">")
    cmd = "(pytest -h >/dev/null)"
    _run_tests(ctx, stage, cmd)


@task
def run_fast_tests(ctx, stage="dev", pytest_opts=""):  # type: ignore
    _LOG.info(">")
    run_tests_dir = "devops/docker_scripts"
    cmd = f"{run_tests_dir}/run_fast_tests.sh {pytest_opts}"
    _run_tests(ctx, stage, cmd)


@task
def run_slow_tests(ctx, stage="dev", pytest_opts=""):  # type: ignore
    _LOG.info(">")
    run_tests_dir = "devops/docker_scripts"
    cmd = f"{run_tests_dir}/run_slow_tests.sh {pytest_opts}"
    _run_tests(ctx, stage, cmd)


@task
def run_superslow_tests(ctx, stage="dev", pytest_opts=""):  # type: ignore
    _LOG.info(">")
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
    _LOG.info(">")
    ecr_base_path = "083233266530.dkr.ecr.us-east-2.amazonaws.com"
    dev_tools_image_prod = f"{ecr_base_path}/dev_tools:prod"
    docker_login(ctx)
    cmd = f"docker pull {dev_tools_image_prod}"
    ctx.run(cmd, pty=True)


@task
def lint(ctx, modified=True, branch=False, files="", phases=""):  # type: ignore
    """
    Lint files.

    :param modified: select the files modified in the client
    :param branch: select the files modified in the current branch
    :param files: specify a space-separated list of files
    :param phases: specify the lint phases to execute
    """
    _LOG.info(">")
    if modified:
        files = git.get_modified_files()
        files = " ".join(files)
    elif branch:
        cmd = "git diff --name-only master..."
        files = hsyste.system_to_string(cmd)[1]
        files = " ".join(files.split("\n"))
    dbg.dassert_isinstance(files, str)
    dbg.dassert_ne(files, "")
    _LOG.info("Files to lint:\n%s", "\n".join(files.split("\n")))
    cmd = (
        f"pre-commit.sh run {phases} --files {files} 2>&1 "
        + "| tee linter_warnings.txt"
    )
    ctx.run(cmd)
