"""
Import as:

import lib_tasks as ltasks
"""

import datetime
import functools
import glob
import json
import logging
import os
import pprint
import re
import sys
from typing import Any, Dict, List, Match, Optional

from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.dbg as dbg
import helpers.git as git
import helpers.introspection as hintros
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.table as htable
import helpers.unit_test as hut
import helpers.version as hversi

# TODO(gp): -> helpers.lib_tasks so we can share across repos (e.g., dev_tools)
# TODO(gp): Do we need to move / rename test_tasks.py?

_LOG = logging.getLogger(__name__)

# #############################################################################
# Default params.
# #############################################################################

# By default we run against the dev image.
_STAGE = "dev"

# This is used to inject the default params.
# TODO(gp): Using a singleton here is not elegant but simple.
_DEFAULT_PARAMS = {}


def set_default_params(params: Dict[str, Any]) -> None:
    global _DEFAULT_PARAMS
    _DEFAULT_PARAMS = params
    _LOG.debug("Assigning:\n%s", pprint.pformat(params))


def get_default_param(key: str) -> Any:
    dbg.dassert_in(key, _DEFAULT_PARAMS)
    dbg.dassert_isinstance(key, str)
    return _DEFAULT_PARAMS[key]


def has_default_param(key: str) -> bool:
    return key in _DEFAULT_PARAMS


def reset_default_params() -> None:
    params: Dict[str, Any] = {}
    set_default_params(params)


# #############################################################################
# Utils.
# #############################################################################

# Since it's not easy to add global command line options to invoke, we piggy
# back the option that already exists.
# If one uses the debug option for `invoke` we turn off the code debugging.
# TODO(gp): Check http://docs.pyinvoke.org/en/1.0/concepts/library.html#
#   modifying-core-parser-arguments
if ("-d" in sys.argv) or ("--debug" in sys.argv):
    dbg.init_logger(verbosity=logging.DEBUG)
else:
    dbg.init_logger(verbosity=logging.INFO)


# NOTE: We need to use a `# type: ignore` for all the @task functions because
# pyinvoke infers the argument type from the code and mypy annotations confuse
# it (see https://github.com/pyinvoke/invoke/issues/357).

# In the following, when using `lru_cache`, we use functions from `hsyste`
# instead of `ctx.run()` since otherwise `lru_cache` would cache `ctx`.

# We prefer not to cache functions running `git` to avoid stale values if we
# call git (e.g., if we cache Git hash and then we do a `git pull`).


def _report_task(txt: str = "") -> None:
    if hut.in_unit_test_mode():
        # In unit test don't print anything.
        return
    func_name = hintros.get_function_name(count=1)
    msg = "## %s: %s" % (func_name, txt)
    # TODO(gp): Do not print during unit tests.
    print(hprint.color_highlight(msg, color="purple"))


# TODO(gp): Pass through command line using a global switch or an env var.
use_one_line_cmd = False


# TODO(gp): Move this to helpers.system_interaction and allow to add the switch
#  globally.
def _remove_spaces(cmd: str) -> str:
    cmd = cmd.rstrip().lstrip()
    cmd = re.sub(r" \\\s*$", " ", cmd, flags=re.MULTILINE)
    cmd = " ".join(cmd.split())
    return cmd


def _run(ctx: Any, cmd: str, *args: Any, **kwargs: Any) -> None:
    _LOG.debug("cmd=%s", cmd)
    if use_one_line_cmd:
        cmd = _remove_spaces(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, *args, **kwargs)


# #############################################################################
# Set-up.
# #############################################################################


@task
def print_setup(ctx):  # type: ignore
    """
    Print some configuration variables.
    """
    _report_task()
    _ = ctx
    var_names = "ECR_BASE_PATH BASE_IMAGE".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_param(v)))


# #############################################################################
# Git.
# #############################################################################


@task
def git_pull(ctx):  # type: ignore
    """
    Pull all the repos.
    """
    _report_task()
    cmd = "git pull --autostash"
    _run(ctx, cmd)
    cmd = "git submodule foreach 'git pull --autostash'"
    _run(ctx, cmd)


@task
def git_pull_master(ctx):  # type: ignore
    """
    Pull master without changing branch.
    """
    _report_task()
    cmd = "git fetch origin master:master"
    _run(ctx, cmd)


@task
def git_merge_master(ctx):  # type: ignore
    """
    Merge `origin/master` into this branch.
    """
    _report_task()
    # TODO(gp): Check that we are in a branch and that the branch is clean.
    git_pull_master(ctx)
    #
    cmd = "git merge master"
    _run(ctx, cmd)


# TODO(gp): Add git_co(ctx)
# Reuse git.git_stash_push() and git.stash_apply()
# git stash save your-file-name
# git checkout master
# # do whatever you had to do with master
# git checkout staging
# git stash pop


@task
def git_clean(ctx):  # type: ignore
    """
    Clean the repo and its submodules.
    """
    _report_task()
    # TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
    #  mistake.
    cmd = "git clean -fd"
    _run(ctx, cmd)
    cmd = "git submodule foreach 'git clean -fd'"
    _run(ctx, cmd)
    # pylint: disable=line-too-long
    cmd = r"""find . | \
    grep -E "(tmp.joblib.unittest.cache|.pytest_cache|.mypy_cache|.ipynb_checkpoints|__pycache__|\.pyc|\.pyo$$)" | \
    xargs rm -rf"""
    # pylint: enable=line-too-long
    _run(ctx, cmd)


@task
def git_branch_files(ctx):  # type: ignore
    """
    Report which files are changed in the current branch with respect to
    master.
    """
    _report_task()
    cmd = "git diff --name-only master..."
    _run(ctx, cmd)


@task
def git_delete_merged_branches(ctx, confirm_delete=True):  # type: ignore
    """
    Remove (both local and remote) branches that have been merged into master.
    """
    _report_task()
    #
    cmd = "git fetch --all --prune"
    _run(ctx, cmd)
    dbg.dassert(
        git.get_branch_name(),
        "master",
        "You need to be on master to delete dead branches",
    )

    def _delete_branches(tag: str) -> None:
        _, txt = hsinte.system_to_string(find_cmd, abort_on_error=False)
        branches = hsinte.text_to_list(txt)
        # Print info.
        _LOG.info(
            "There are %d %s branches to delete:\n%s",
            len(branches),
            tag,
            "\n".join(branches),
        )
        if not branches:
            # No branch to delete, then we are done.
            return
        # Ask whether to continue.
        if confirm_delete:
            hsinte.query_yes_no(
                dbg.WARNING + f": Delete these {tag} branches?", abort_on_no=True
            )
        for branch in branches:
            cmd_tmp = f"{delete_cmd} {branch}"
            _run(ctx, cmd_tmp)

    # Delete local branches that are already merged into master.
    # > git branch --merged
    # * AmpTask1251_Update_GH_actions_for_amp_02
    find_cmd = r"git branch --merged master | grep -v master | grep -v \*"
    delete_cmd = "git branch -d"
    _delete_branches("local")
    # Get the branches to delete.
    find_cmd = (
        "git branch -r --merged origin/master"
        + r" | grep -v master | sed 's/origin\///'"
    )
    delete_cmd = "git push origin --delete"
    _delete_branches("remote")
    #
    cmd = "git fetch --all --prune"
    _run(ctx, cmd)


@task
def git_create_branch(ctx, branch_name=""):  # type: ignore
    """
    Create and push upstream a branch called `branch_name`.

    E.g., > git checkout -b
    LemTask169_Get_GH_actions_working_on_lemonade > git push --set-
    upstream origin LemTask169_Get_GH_actions_working_on_lemonade
    """
    _report_task()
    dbg.dassert_eq(
        git.get_branch_name(),
        "master",
        "Typically you should branch from `master`",
    )
    # Fetch master.
    cmd = "git pull --autostash"
    _run(ctx, cmd)
    # git checkout -b LemTask169_Get_GH_actions_working_on_lemonade
    cmd = f"git checkout -b {branch_name}"
    _run(ctx, cmd)
    # git push --set-upstream origin LemTask169_Get_GH_actions_working_on_lemonade
    cmd = f"git push --set-upstream origin {branch_name}"
    _run(ctx, cmd)


# TODO(gp): Add dev_scripts/git/git_create_patch*.sh
# dev_scripts/git/git_backup.sh
# dev_scripts/git/gcl
# dev_scripts/git/gd_master.sh
# dev_scripts/git/git_branch.sh
# dev_scripts/git/git_branch_point.sh

# #############################################################################
# Docker.
# #############################################################################


@task
def docker_images_ls_repo(ctx):  # type: ignore
    """
    List images in the logged in repo.
    """
    _report_task()
    docker_login(ctx)
    ecr_base_path = get_default_param("ECR_BASE_PATH")
    _run(ctx, f"docker image ls {ecr_base_path}")


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
    # pylint: enable=line-too-long
    fmt = (
        r"""table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}"""
        + r"\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}"
        + r'\t{{.Label "com.docker.compose.service"}}'
    )
    cmd = f"docker ps --format='{fmt}'"
    cmd = _remove_spaces(cmd)
    _run(ctx, cmd)


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
    # pylint: enable=line-too-long
    _report_task()
    fmt = (
        r"table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
        + r"\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}"
    )
    cmd = f"docker stats --no-stream --format='{fmt}'"
    _run(ctx, cmd)


@task
def docker_kill_last(ctx):  # type: ignore
    """
    Kill the last Docker container started.
    """
    _report_task()
    _run(ctx, "docker ps -l")
    _run(ctx, "docker rm -f $(docker ps -l -q)")


@task
def docker_kill_all(ctx):  # type: ignore
    """
    Kill all the Docker containers.
    """
    _report_task()
    _run(ctx, "docker ps -a")
    _run(ctx, "docker rm -f $(docker ps -a -q)")


# docker system prune
# docker container ps -f "status=exited"
# docker container rm $(docker container ps -f "status=exited" -q)
# docker rmi $(docker images --filter="dangling=true" -q)

# pylint: disable=line-too-long
# Remove the images with hash
# > docker image ls
# REPOSITORY                                               TAG                                        IMAGE ID       CREATED         SIZE
# 083233266530.dkr.ecr.us-east-2.amazonaws.com/im          07aea615a2aa9290f7362e99e1cc908876700821   d0889bf972bf   6 minutes ago   684MB
# 083233266530.dkr.ecr.us-east-2.amazonaws.com/im          rc                                         d0889bf972bf   6 minutes ago   684MB
# python                                                   3.7-slim-buster                            e7d86653f62f   14 hours ago    113MB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools   ce789e4718175fcdf6e4857581fef1c2a5ee81f3   2f64ade2c048   14 hours ago    2.02GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools   local                                      2f64ade2c048   14 hours ago    2.02GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools   d401a2a0bef90b9f047c65f8adb53b28ba05d536   1b11bf234c7f   15 hours ago    2.02GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools   52ccd63edbc90020f450c074b7c7088a1806c5ac   90b70a55c367   15 hours ago    1.95GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools   2995608a7d91157fc1a820869a6d18f018c3c598   0cb3858e85c6   15 hours ago    2.01GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp         415376d58001e804e840bf3907293736ad62b232   e6ea837ab97f   18 hours ago    1.65GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp         dev                                        e6ea837ab97f   18 hours ago    1.65GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp         local                                      e6ea837ab97f   18 hours ago    1.65GB
# 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp         9586cc2de70a4075b9fdcdb900476f8a0f324e3e   c75d2447da79   18 hours ago    1.65GB
# pylint: enable=line-too-long


# #############################################################################
# Docker development.
# #############################################################################

# TODO(gp):
# We might want to organize the code in a base class using a Command pattern,
# so that it's easier to generalize the code for multiple repos.
#
# class DockerCommand:
#   def pull():
#     ...
#   def cmd():
#     ...
#
# For now we pass the customizable part through the default params.


@task
def docker_pull(ctx, stage=_STAGE, images="all"):  # type: ignore
    """
    Pull images from the registry.
    """
    _report_task()
    docker_login(ctx)
    # Default is all the images.
    if images == "all":
        images = "current dev_tools"
    # Parse the images.
    image_tokens = [token.rstrip().lstrip() for token in images.split()]
    _LOG.info("image_tokens=%s", ", ".join(image_tokens))
    #
    for token in image_tokens:
        if token == "":
            continue
        if token == "current":
            base_image = ""
            image = _get_image(stage, base_image)
        elif token == "dev_tools":
            image = get_default_param("DEV_TOOLS_IMAGE_PROD")
        else:
            raise ValueError("Can't recognize image token '%s'" % token)
        _LOG.info("token='%s': image='%s'", token, image)
        _check_image(image)
        cmd = f"docker pull {image}"
        _run(ctx, cmd, pty=True)


@functools.lru_cache()
def _get_aws_cli_version() -> int:
    # > aws --version
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    cmd = "aws --version"
    res = hsinte.system_to_one_line(cmd)[1]
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
    """
    Log in the AM Docker repo on AWS.
    """
    _report_task()
    if "CI" in os.environ:
        _LOG.warning("Running inside GitHub Action: skipping `docker_login`")
        return
    major_version = _get_aws_cli_version()
    # TODO(gp): We should get this programmatically from ~/aws/.credentials
    region = "us-east-1"
    if major_version == 1:
        cmd = f"eval $(aws ecr get-login --no-include-email --region {region})"
    else:
        ecr_base_path = get_default_param("ECR_BASE_PATH")
        cmd = (
            f"docker login -u AWS -p $(aws ecr get-login --region {region}) "
            + f"https://{ecr_base_path}"
        )
    _run(ctx, cmd)


def _get_base_docker_compose_path() -> str:
    """
    Return the base docker compose `devops/compose/docker-compose.yml`.
    """
    # Add the default path.
    dir_name = "devops/compose"
    # TODO(gp): Factor out the piece below.
    docker_compose_path = "docker-compose.yml"
    docker_compose_path = os.path.join(dir_name, docker_compose_path)
    docker_compose_path = os.path.abspath(docker_compose_path)
    return docker_compose_path


def _get_amp_docker_compose_path() -> Optional[str]:
    """
    Return the docker compose for `amp` as supermodule or as submodule.

    E.g., `devops/compose/docker-compose_as_submodule.yml` and
    `devops/compose/docker-compose_as_supermodule.yml`
    """
    path = git.get_path_from_supermodule()
    if path != "":
        _LOG.warning("amp is a submodule")
        docker_compose_path = "docker-compose_as_submodule.yml"
        # Add the default path.
        dir_name = "devops/compose"
        docker_compose_path = os.path.join(dir_name, docker_compose_path)
        docker_compose_path = os.path.abspath(docker_compose_path)
    else:
        docker_compose_path = None
    return docker_compose_path


def _get_git_hash() -> str:
    cmd = "git rev-parse HEAD"
    git_hash: str = hsinte.system_to_one_line(cmd)[1]
    _LOG.debug("git_hash=%s", git_hash)
    return git_hash


_INTERNET_ADDRESS_RE = r"([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
_IMAGE_RE = r"[a-z0-9_-]+"
_TAG_RE = r"[a-z0-9_-]+"


def _check_image(image: str) -> None:
    """
    An image should look like:

    665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local
    """
    m = re.match(rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_RE}:{_TAG_RE}$", image)
    dbg.dassert(m, "Invalid image: '%s'", image)


def _check_base_image(base_image: str) -> None:
    """
    A base image should look like.

    665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    regex = rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_RE}$"
    _LOG.debug("regex=%s", regex)
    m = re.match(regex, base_image)
    dbg.dassert(m, "Invalid base_image: '%s'", base_image)


def _get_base_image(base_image: str) -> str:
    """
    :return: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    if base_image == "":
        # TODO(gp): Use os.path.join.
        base_image = (
            get_default_param("ECR_BASE_PATH")
            + "/"
            + get_default_param("BASE_IMAGE")
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


def _to_cmd(docker_cmd_: List[str]) -> str:
    r"""
    Convert a command encoded as a list of strings into a single command
    separated by `\`.

    E.g., convert
    ```
        ['IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:dev',
            '\n        docker-compose',
            '\n        --file amp/devops/compose/docker-compose.yml',
            '\n        --file amp/devops/compose/docker-compose_as_submodule.yml',
            '\n        --env-file devops/env/default.env']
        ```
    into
        ```
        docker_cmd=IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:dev \
            docker-compose \
            --file devops/compose/docker-compose.yml \
            --file devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env
        ```
    """
    # Expand all strings into single lines.
    _LOG.debug("docker_cmd=%s", docker_cmd_)
    docker_cmd_tmp = []
    for dc in docker_cmd_:
        # Add a `\` at the end of each string.
        dbg.dassert(not dc.endswith("\\"), "dc='%s'", dc)
        dc += " \\"
        docker_cmd_tmp.extend(dc.split("\n"))
    docker_cmd_ = docker_cmd_tmp
    # Remove empty lines.
    docker_cmd_ = [cmd for cmd in docker_cmd_ if cmd.rstrip().lstrip() != ""]
    # Package the command.
    docker_cmd_ = "\n".join(docker_cmd_)
    # Remove a `\` at the end, since it is not needed.
    docker_cmd_ = docker_cmd_.rstrip("\\")
    _LOG.debug("docker_cmd=%s", docker_cmd_)
    return docker_cmd_


def _get_docker_cmd(
    stage: str,
    base_image: str,
    cmd: str,
    extra_env_vars: Optional[List[str]] = None,
    extra_docker_compose_files: Optional[List[str]] = None,
    extra_docker_run_opts: Optional[List[str]] = None,
    service_name: str = "app",
    entrypoint: bool = True,
    print_docker_config: bool = False,
) -> str:
    """
    :param base_image: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    :param extra_env_vars: represent vars to add, e.g., `["PORT=9999", "DRY_RUN=1"]`
    :param print_config: print the docker config for debugging purposes
    """
    hprint.log(
        _LOG,
        logging.DEBUG,
        "stage base_image cmd extra_env_vars"
        " extra_docker_compose_files extra_docker_run_opts"
        " service_name entrypoint",
    )
    docker_cmd_: List[str] = []
    # - Handle the image.
    image = _get_image(stage, base_image)
    _LOG.debug("base_image=%s stage=%s -> image=%s", base_image, stage, image)
    _check_image(image)
    docker_cmd_.append(f"IMAGE={image}")
    # - Handle extra env vars.
    if extra_env_vars:
        dbg.dassert_isinstance(extra_env_vars, list)
        for env_var in extra_env_vars:
            docker_cmd_.append(f"{env_var}")
    #
    docker_cmd_.append(
        r"""
        docker-compose"""
    )
    # - Handle the docker compose files.
    docker_compose_files = []
    docker_compose_files.append(_get_base_docker_compose_path())
    docker_compose_file_tmp = _get_amp_docker_compose_path()
    if docker_compose_file_tmp:
        docker_compose_files.append(docker_compose_file_tmp)
    # Add the compose files from command line.
    if extra_docker_compose_files:
        dbg.dassert_isinstance(extra_docker_compose_files, list)
        docker_compose_files.extend(extra_docker_compose_files)
    # Add the compose files from the global params.
    key = "DOCKER_COMPOSE_FILES"
    if has_default_param(key):
        docker_compose_files.append(get_default_param(key))
    #
    _LOG.debug(hprint.to_str("docker_compose_files"))
    for docker_compose in docker_compose_files:
        dbg.dassert_exists(docker_compose)
    file_opts = " ".join([f"--file {dcf}" for dcf in docker_compose_files])
    _LOG.debug(hprint.to_str("file_opts"))
    docker_cmd_.append(
        rf"""
        {file_opts}"""
    )
    # - Handle the env file.
    env_file = "devops/env/default.env"
    docker_cmd_.append(
        rf"""
        --env-file {env_file}"""
    )
    # - Add the `config` command for debugging purposes.
    docker_config_cmd : List[str] = docker_cmd_[:]
    docker_config_cmd.append(
        r"""
        config"""
    )
    # - Add the `run` command.
    docker_cmd_.append(
        r"""
        run \
        --rm"""
    )
    # - Handle the user.
    user_name = hsinte.get_user_name()
    docker_cmd_.append(
        rf"""
        -l user={user_name}"""
    )
    # - Handle the extra docker options.
    if extra_docker_run_opts:
        dbg.dassert_isinstance(extra_docker_run_opts, list)
        extra_opts = " ".join(extra_docker_run_opts)
        docker_cmd_.append(
            rf"""
        {extra_opts}"""
        )
    # - Handle entrypoint.
    if entrypoint:
        docker_cmd_.append(
            rf"""
        {service_name}"""
        )
        if cmd:
            docker_cmd_.append(
                rf"""
        {cmd}"""
            )
    else:
        docker_cmd_.append(
            rf"""
        --entrypoint bash \
        {service_name}"""
        )
    # Print the config for debugging purpose.
    if print_docker_config:
        docker_config_cmd = _to_cmd(docker_config_cmd)
        _LOG.debug("docker_config_cmd=\n%s", docker_config_cmd)
        _LOG.debug(
            "docker_config=\n%s", hsinte.system_to_string(docker_config_cmd)[1]
        )
    # Print the config for debugging purpose.
    docker_cmd_ = _to_cmd(docker_cmd_)
    return docker_cmd_


def _docker_cmd(
    ctx: Any,
    docker_cmd_: str,
) -> None:
    """
    :param base_image: e.g., 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _LOG.debug("cmd=%s", docker_cmd_)
    _run(ctx, docker_cmd_, pty=True)


@task
def docker_bash(ctx, stage=_STAGE, entrypoint=True):  # type: ignore
    """
    Start a bash shell inside the container corresponding to a stage.
    """
    _report_task()
    base_image = ""
    cmd = "bash"
    docker_cmd_ = _get_docker_cmd(stage, base_image, cmd, entrypoint=entrypoint)
    _docker_cmd(ctx, docker_cmd_)


@task
def docker_cmd(ctx, stage=_STAGE, cmd=""):  # type: ignore
    """
    Execute the command `cmd` inside a container corresponding to a stage.
    """
    _report_task()
    dbg.dassert_ne(cmd, "")
    base_image = ""
    # TODO(gp): Do we need to overwrite the entrypoint?
    docker_cmd_ = _get_docker_cmd(stage, base_image, cmd)
    _docker_cmd(ctx, docker_cmd_)


def _get_docker_jupyter_cmd(
    stage: str,
    base_image: str,
    port: int,
    self_test: bool,
    print_docker_config: bool = False,
) -> str:
    cmd = ""
    extra_env_vars = [f"PORT={port}"]
    extra_docker_run_opts = ["--service-ports"]
    service_name = "jupyter_server_test" if self_test else "jupyter_server"
    #
    docker_cmd_ = _get_docker_cmd(
        stage,
        base_image,
        cmd,
        extra_env_vars=extra_env_vars,
        extra_docker_run_opts=extra_docker_run_opts,
        service_name=service_name,
        print_docker_config=print_docker_config,
    )
    return docker_cmd_


@task
def docker_jupyter(  # type: ignore
    ctx,
    stage=_STAGE,
    base_image="",
    port=9999,
    self_test=False,
):
    """
    Run jupyter notebook server.
    """
    _report_task()
    #
    docker_cmd_ = _get_docker_jupyter_cmd(stage, base_image, port, self_test)
    _docker_cmd(ctx, docker_cmd_)


# #############################################################################
# Images workflows.
# #############################################################################


def _to_abs_path(filename: str) -> str:
    filename = os.path.abspath(filename)
    dbg.dassert_exists(filename)
    return filename


# Use Docker buildkit or not.
# DOCKER_BUILDKIT = 1
DOCKER_BUILDKIT = 0


@functools.lru_cache()
def _get_build_tag() -> str:
    """
    Return a string to tag the build.

    E.g.,
    build_tag=1.0.0-20210428-
        AmpTask1280_Use_versioning_to_keep_code_and_container_in_sync-
        500a9e31ee70e51101c1b2eb82945c19992fa86e
    """
    dir_name = os.path.dirname(os.path.abspath(__file__))
    code_ver = hversi.get_code_version(dir_name)
    # We can't use datetime_.get_timestamp() since we don't want to pick up
    # the dependencies from pandas.
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    branch_name = git.get_branch_name()
    hash_ = git.get_head_hash()
    build_tag = f"{code_ver}-{timestamp}-{branch_name}-{hash_}"
    return build_tag


# DEV image flow:
# - A "local" image (which is a release candidate for the DEV image) is built
# - A qualification process (e.g., running all tests) is performed on the "local"
#   image (typically through GitHub actions)
# - If qualification is passed, it becomes "latest".


# For base_image, we use "" as default instead None since pyinvoke can only infer
# a single type.
@task
def docker_build_local_image(  # type: ignore
    ctx, cache=True, base_image="", update_poetry=False
):
    """
    Build a local as a release candidate image.

    :param update_poetry: run poetry lock to update the packages
    :param cache: use the cache
    """
    _report_task()
    # Update poetry.
    if update_poetry:
        cmd = "cd devops/docker_build/; poetry lock"
        _run(ctx, cmd)
    #
    image_local = _get_image("local", base_image)
    image_hash = _get_image("hash", base_image)
    #
    _check_image(image_local)
    _check_image(image_hash)
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no_cache" if not cache else ""
    # The container version is the version used from this code.
    container_version = hversi.get_code_version()
    build_tag = _get_build_tag()
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        --build-arg CONTAINER_VERSION={container_version} \
        --build-arg BUILD_TAG={build_tag} \
        --tag {image_local} \
        --tag {image_hash} \
        --file {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


@task
def docker_push_local_image_to_dev(ctx, base_image=""):  # type: ignore
    """
    (ONLY CI/CD) Mark the "local" image as "dev" and "latest" and push to ECR.
    """
    _report_task()
    docker_login(ctx)
    #
    image_local = _get_image("local", base_image)
    cmd = f"docker push {image_local}"
    _run(ctx, cmd)
    #
    image_hash = _get_image("hash", base_image)
    cmd = f"docker tag {image_local} {image_hash}"
    _run(ctx, cmd)
    cmd = f"docker push {image_hash}"
    _run(ctx, cmd)
    #
    image_dev = _get_image("dev", base_image)
    cmd = f"docker tag {image_local} {image_dev}"
    _run(ctx, cmd)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd)


@task
def docker_release_dev_image(  # type: ignore
    ctx,
    cache=True,
    skip_tests=False,
    run_fast=True,
    run_slow=True,
    run_superslow=False,
    push_to_repo=True,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest "dev" image.

    This can be used to test the entire flow from scratch by building an image,
    running the tests, but not necessarily pushing.

    :param: just_build skip all the tests and release the dev image.
    """
    _report_task()
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        run_fast = run_slow = run_superslow = False
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
    if push_to_repo:
        docker_push_local_image_to_dev(ctx)
    else:
        _LOG.warning("Skipping pushing image to repo as requested")
    _LOG.info("==> SUCCESS <==")


# PROD image flow:
# - PROD image has no release candidate
# - The DEV image is qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image is tagged as "prod"


# TODO(gp): Remove redundancy with docker_build_local_image().
@task
def docker_build_prod_image(ctx, cache=False, base_image=""):  # type: ignore
    """
    (ONLY CI/CD) Build a prod image.
    """
    _report_task()
    image_prod = _get_image("prod", base_image)
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
    (ONLY CI/CD) Build, test, and release to ECR the prod image.
    """
    _report_task()
    # Build dev image.
    docker_build_local_image(ctx, cache=cache)
    docker_push_local_image_to_dev(ctx)
    # Build prod image.
    docker_build_prod_image(ctx, cache=cache)
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
    (ONLY CI/CD) Release both dev and prod image to ECR.
    """
    _report_task()
    docker_release_dev_image(ctx)
    docker_release_prod_image(ctx)
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# Run tests.
# #############################################################################

_COV_PYTEST_OPTS = [
    # Only compute coverage for current project and not venv libraries.
    "--cov=.",
    "--cov-branch",
    # Report the missing lines.
    # Name                 Stmts   Miss  Cover   Missing
    # -------------------------------------------------------------------------
    # myproj/__init__          2      0   100%
    # myproj/myproj          257     13    94%   24-26, 99, 149, 233-236, 297-298
    "--cov-report term-missing",
    # Report data in the directory `htmlcov`.
    "--cov-report html",
    # "--cov-report annotate",
]


@task
def run_blank_tests(ctx, stage=_STAGE):  # type: ignore
    """
    (ONLY CI/CD) Test that pytest in the container works.
    """
    _report_task()
    base_image = ""
    cmd = '"pytest -h >/dev/null"'
    docker_cmd_ = _get_docker_cmd(stage, base_image, cmd)
    _docker_cmd(ctx, docker_cmd_)


# #############################################################################


def _find_test_files(
    dir_name: Optional[str] = None, use_absolute_path: bool = False
) -> List[str]:
    """
    Find all the files containing test code in `dir_name`.
    """
    dir_name = dir_name or "."
    dbg.dassert_dir_exists(dir_name)
    _LOG.debug("dir_name=%s", dir_name)
    # Find all the file names containing test code.
    _LOG.info("Searching from '%s'", dir_name)
    path = os.path.join(dir_name, "**", "test_*.py")
    _LOG.debug("path=%s", path)
    file_names = glob.glob(path, recursive=True)
    _LOG.debug("Found %d files: %s", len(file_names), str(file_names))
    dbg.dassert_no_duplicates(file_names)
    # Test files should always under a dir called `test`.
    for file_name in file_names:
        if "/old/" in file_name:
            continue
        dbg.dassert_eq(
            os.path.basename(os.path.dirname(file_name)),
            "test",
            "Test file '%s' needs to be under a `test` dir ",
            file_name,
        )
        dbg.dassert_not_in(
            "notebook/",
            file_name,
            "Test file '%s' should not be under a `notebook` dir",
            file_name,
        )
    # Make path relatives, if needed.
    if use_absolute_path:
        file_names = [os.path.abspath(file_name) for file_name in file_names]
    #
    file_names = sorted(file_names)
    _LOG.debug("file_names=%s", file_names)
    dbg.dassert_no_duplicates(file_names)
    return file_names


def _find_test_class(class_name: str, file_names: List[str]) -> List[str]:
    """
    Find test file containing the class `class_name` and report it in a format
    compatible with pytest.

    E.g., for "TestLibTasksRunTests1" return
    "test/test_lib_tasks.py::TestLibTasksRunTests1"
    """
    # > jackpy TestLibTasksRunTests1
    # test/test_lib_tasks.py:60:class TestLibTasksRunTests1(hut.TestCase):
    regex = r"^\s*class\s+(%s)\(" % re.escape(class_name)
    _LOG.debug("regex='%s'", regex)
    res: List[str] = []
    # Scan all the files.
    for file_name in file_names:
        _LOG.debug("file_name=%s", file_name)
        txt = hio.from_file(file_name)
        # Search for the class in each file.
        for i, line in enumerate(txt.split("\n")):
            # _LOG.debug("file_name=%s i=%s: %s", file_name, i, line)
            # TODO(gp): We should skip ```, """, '''
            m = re.match(regex, line)
            if m:
                found_class_name = m.group(1)
                _LOG.debug("  %s:%d -> %s", line, i, found_class_name)
                res_tmp = f"{file_name}::{found_class_name}"
                _LOG.debug("res_tmp=%s", res_tmp)
                res.append(res_tmp)
    res = sorted(list(set(res)))
    return res


@task
def find_test_class(ctx, class_name="", dir_name="."):  # type: ignore
    """
    Report test files containing `class_name` in a format compatible with
    pytest.

    :param class_name: the class to search
    :param dir_name: the dir from which to search (default: .)
    """
    _report_task()
    dbg.dassert(class_name != "", "You need to specify a class name")
    _ = ctx
    file_names = _find_test_files(dir_name)
    res = _find_test_class(class_name, file_names)
    print(res)


# #############################################################################


# TODO(gp): decorator_name -> pytest_mark
def _find_test_decorator(decorator_name: str, file_names: List[str]) -> List[str]:
    """
    Find test files containing tests with a certain decorator
    `@pytest.mark.XYZ`.
    """
    dbg.dassert_isinstance(file_names, list)
    # E.g.,
    #   @pytest.mark.slow(...)
    #   @pytest.mark.no_container
    string = "@pytest.mark.%s" % decorator_name
    regex = r"^\s*%s\s*[\(]?" % re.escape(string)
    _LOG.debug("regex='%s'", regex)
    res: List[str] = []
    # Scan all the files.
    for file_name in file_names:
        _LOG.debug("file_name=%s", file_name)
        txt = hio.from_file(file_name)
        # Search for the class in each file.
        for i, line in enumerate(txt.split("\n")):
            # _LOG.debug("file_name=%s i=%s: %s", file_name, i, line)
            # TODO(gp): We should skip ```, """, '''. We can add a function to
            # remove all the comments, although we need to keep track of the
            # line original numbers.
            m = re.match(regex, line)
            if m:
                _LOG.debug("  -> found: %d:%s", i, line)
                res.append(file_name)
    #
    res = sorted(list(set(res)))
    return res


@task
def find_test_decorator(ctx, decorator_name="", dir_name="."):  # type: ignore
    """
    Report test files containing `class_name` in a format compatible with
    pytest.

    :param class_name: the class to search
    :param dir_name: the dir from which to search
    """
    _report_task()
    dbg.dassert(decorator_name != "", "You need to specify a decorator name")
    _ = ctx
    file_names = _find_test_files(dir_name)
    res = _find_test_class(decorator_name, file_names)
    print(res)


# #############################################################################


def _build_run_command_line(
    pytest_opts: str,
    pytest_mark: str,
    dir_name: str,
    skip_submodules: bool,
    coverage: bool,
    collect_only: bool,
    #
    skipped_tests: str,
) -> str:
    """
    Same params as run_fast_tests().

    :param skipped_tests: -m option for pytest
    """
    pytest_opts_tmp = []
    if pytest_opts != "":
        pytest_opts_tmp.append(pytest_opts)
    if skipped_tests != "":
        pytest_opts_tmp.insert(0, f'-m "{skipped_tests}"')
    dir_name = dir_name or "."
    file_names = _find_test_files(dir_name)
    _LOG.debug("file_names=%s", file_names)
    if pytest_mark != "":
        file_names = _find_test_decorator(pytest_mark, file_names)
        _LOG.debug(
            "After pytest_mark='%s': file_names=%s", pytest_mark, file_names
        )
        pytest_opts_tmp.extend(file_names)
    if skip_submodules:
        submodule_paths = git.get_submodule_paths()
        _LOG.warning(
            "Skipping %d submodules: %s", len(submodule_paths), submodule_paths
        )
        pytest_opts_tmp.append(
            " ".join(["--ignore %s" % path for path in submodule_paths])
        )
    if coverage:
        pytest_opts_tmp.append(" ".join(_COV_PYTEST_OPTS))
    if collect_only:
        _LOG.warning("Only collecting tests as per user request")
        pytest_opts_tmp.append("--collect-only")
    # Concatenate the options.
    _LOG.debug("pytest_opts_tmp=\n%s", str(pytest_opts_tmp))
    pytest_opts_tmp = [po for po in pytest_opts_tmp if po != ""]
    pytest_opts = " ".join([po.rstrip().lstrip() for po in pytest_opts_tmp])
    cmd = f"pytest {pytest_opts}"
    return cmd


def _run_test_cmd(
    ctx: Any,
    stage: str,
    cmd: str,
    coverage: bool,
    collect_only: bool,
) -> None:
    if collect_only:
        # Clean files.
        _run(ctx, "rm -rf ./.coverage*")
    # Run.
    base_image = ""
    # We need to add some " to pass the string as it is to the container.
    cmd = f"'{cmd}'"
    docker_cmd_ = _get_docker_cmd(stage, base_image, cmd)
    _docker_cmd(ctx, docker_cmd_)
    # Print message about coverage.
    if coverage:
        msg = """- The coverage results in textual form are above.

- To browse the files annotate with coverage, start a server (not from the container):
  > (cd ./htmlcov; python -m http.server 33333)
  then go with your browser to `localhost:33333`
"""
        print(msg)


def _run_tests(
    ctx: Any,
    stage: str,
    pytest_opts: str,
    pytest_mark: str,
    dir_name: str,
    skip_submodules: bool,
    coverage: bool,
    collect_only: bool,
    skipped_tests: str,
) -> None:
    # Build the command line.
    cmd = _build_run_command_line(
        pytest_opts,
        pytest_mark,
        dir_name,
        skip_submodules,
        coverage,
        collect_only,
        skipped_tests,
    )
    # Execute the command line.
    _run_test_cmd(
        ctx,
        stage,
        cmd,
        coverage,
        collect_only,
    )


# TODO(gp): Pass a test_list in fast, slow, ... instead of duplicating all the code.
@task
def run_fast_tests(  # type: ignore
    ctx,
    stage=_STAGE,
    pytest_opts="",
    pytest_mark="",
    dir_name="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
):
    """
    Run fast tests.

    :param stage: select a specific stage for the Docker image
    :param pytest_opts: option for pytest
    :param pytest_mark: test list to select as `@pytest.mark.XYZ`
    :param dir_name: dir to start searching for tests
    :param skip_submodules: ignore all the dir inside a submodule
    :param coverage: enable coverage computation
    :param collect_only: do not run tests but show what will be executed
    """
    _report_task()
    skipped_tests = "not slow and not superslow"
    _run_tests(
        ctx,
        stage,
        pytest_opts,
        pytest_mark,
        dir_name,
        skip_submodules,
        coverage,
        collect_only,
        skipped_tests,
    )


@task
def run_slow_tests(  # type: ignore
    ctx,
    stage=_STAGE,
    pytest_opts="",
    pytest_mark="",
    dir_name="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
):
    """
    Run slow tests.
    """
    _report_task()
    skipped_tests = "slow and not superslow"
    _run_tests(
        ctx,
        stage,
        pytest_opts,
        pytest_mark,
        dir_name,
        skip_submodules,
        coverage,
        collect_only,
        skipped_tests,
    )


@task
def run_superslow_tests(  # type: ignore
    ctx,
    stage=_STAGE,
    pytest_opts="",
    pytest_mark="",
    dir_name="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
):
    """
    Run superslow tests.
    """
    _report_task()
    skipped_tests = "not slow and superslow"
    _run_tests(
        ctx,
        stage,
        pytest_opts,
        pytest_mark,
        dir_name,
        skip_submodules,
        coverage,
        collect_only,
        skipped_tests,
    )


@task
def run_fast_slow_tests(  # type: ignore
    ctx,
    stage=_STAGE,
    pytest_opts="",
    pytest_mark="",
    dir_name="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
):
    """
    Run fast and slow tests.
    """
    _report_task()
    skipped_tests = "not superslow"
    _run_tests(
        ctx,
        stage,
        pytest_opts,
        pytest_mark,
        dir_name,
        skip_submodules,
        coverage,
        collect_only,
        skipped_tests,
    )


@task
def jump_to_pytest_error(ctx, log_name=""):  # type: ignore
    """
    Parse the traceback from pytest and navigate it with vim.

    > pyt helpers/test/test_traceback.py
    > invoke jump_to_pytest_error
    # There is a also an alias `ie` for the previous command line.

    > devops/debug/compare.sh 2>&1 | tee log.txt
    > ie -l log.txt

    :param log_name: the file with the traceback
    """
    if not log_name:
        log_name = "tmp.pytest.log"
    _LOG.info("Reading %s", log_name)
    # Convert the traceback into a cfile.
    cmd = f"dev_scripts/traceback_to_cfile.py -i {log_name} -o cfile"
    _run(ctx, cmd)
    # Read and navigate the cfile with vim.
    cmd = 'vim -c "cfile cfile"'
    _run(ctx, cmd, pty=True)


@task
def pytest_clean(ctx):  # type: ignore
    """
    Clean pytest artifacts.
    """
    _report_task()
    _ = ctx
    import helpers.pytest_ as hpytes

    hpytes.pytest_clean(".")


# TODO(gp): Consolidate the code from dev_scripts/testing here.

# #############################################################################
# Linter.
# #############################################################################


@task
def lint(ctx, modified=False, branch=False, files="", phases=""):  # type: ignore
    """
    Lint files.

    :param modified: select the files modified in the client
    :param branch: select the files modified in the current branch
    :param files: specify a space-separated list of files
    :param phases: specify the lint phases to execute
    """
    _report_task()
    dbg.dassert_lte(
        int(modified) + int(branch) + int(files != ""),
        1,
        msg="You can specify only one option among --modified, --branch, or --files",
    )
    if modified:
        files = git.get_modified_files()
        files = " ".join(files)
    elif branch:
        cmd = "git diff --name-only master..."
        files = hsinte.system_to_string(cmd)[1]
        files = " ".join(files.split("\n"))
    #
    dbg.dassert_isinstance(files, str)
    _LOG.debug("files='%s'", str(files))
    files_as_list = files.split(" ")
    files_as_list = [f for f in files_as_list if f != ""]
    if len(files_as_list) == 0:
        dbg.dfatal(
            "You need specify one option among --modified, --branch, or --files"
        )
    dbg.dassert_lte(1, len(files_as_list))
    _LOG.info("Files to lint:\n%s", "\n".join(files_as_list))
    files_as_str = " ".join(files_as_list)
    #
    cmd = (
        f"pre-commit.sh run {phases} --files {files_as_str} 2>&1 "
        + "| tee linter_warnings.txt"
    )
    _run(ctx, cmd)


# TODO(gp): Finish this.
@task
def get_amp_files(ctx):  # type: ignore
    """
    Get some files that need to be copied across repos.
    """
    _report_task()
    _ = ctx
    # TODO(gp): Move this inside `bashrc`.
    token = "***REMOVED***"
    file_names = ["lib_tasks.py"]
    for file_name in file_names:
        cmd = (
            f"wget "
            f"https://raw.githubusercontent.com/alphamatic/amp/master/{file_name}"
            f"?token={token} -O {file_name}"
        )
        hsinte.system(cmd)


# #############################################################################
# GitHub CLI.
# #############################################################################


@task
def gh_workflow_list(ctx, branch="branch", status="all"):  # type: ignore
    """
    Report the status of the GH workflows in a branch.
    """
    _report_task(hprint.to_str("branch status"))
    _ = ctx
    #
    cmd = "export NO_COLOR=1; gh run list"
    # pylint: disable=line-too-long
    # > gh run list
    # ✓  Merge branch 'master' into AmpTask1251_ Slow tests  AmpTask1251_Update_GH_actions_for_amp  pull_request       788984377
    # ✓  Merge branch 'master' into AmpTask1251_ Fast tests  AmpTask1251_Update_GH_actions_for_amp  pull_request       788984376
    # X  Merge branch 'master' into AmpTask1251_ Run linter  AmpTask1251_Update_GH_actions_for_amp  pull_request       788984375
    # X  Fix lint issue                          Fast tests  master                                 workflow_dispatch  788949955
    # pylint: enable=line-too-long
    if branch == "branch":
        branch_name = git.get_branch_name()
    elif branch == "master":
        branch_name = "master"
    elif branch == "all":
        branch_name = None
    else:
        raise ValueError("Invalid mode='%s'" % branch)
    # The output is tab separated. Parse it with csv and then filter.
    _, txt = hsinte.system_to_string(cmd)
    _LOG.debug(hprint.to_str("txt"))
    # completed  success  Merge pull...  Fast tests  master  push  2m18s  792511437
    cols = [
        "status",
        "outcome",
        "descr",
        "workflow",
        "branch",
        "trigger",
        "time",
        "workflow_id",
    ]
    table = htable.Table.from_text(cols, txt, delimiter="\t")
    # table = [line for line in csv.reader(txt.split("\n"), delimiter="\t")]
    _LOG.debug(hprint.to_str("table"))
    #
    if branch != "all":
        field = "branch"
        value = branch_name
        _LOG.info("Filtering table by %s=%s", field, value)
        table = table.filter_rows(field, value)
    #
    if status != "all":
        field = "status"
        value = status
        _LOG.info("Filtering table by %s=%s", field, value)
        table = table.filter_rows(field, value)
    #
    print(str(table))


@task
def gh_workflow_run(ctx, branch="branch", workflows="all"):  # type: ignore
    """
    Run GH workflows in a branch.
    """
    _report_task(hprint.to_str("branch workflows"))
    # Get the branch name.
    if branch == "branch":
        branch_name = git.get_branch_name()
    elif branch == "master":
        branch_name = "master"
    else:
        raise ValueError("Invalid branch='%s'" % branch)
    _LOG.debug(hprint.to_str("branch_name"))
    # Get the workflows.
    if workflows == "all":
        gh_tests = ["fast_tests", "slow_tests"]
    else:
        gh_tests = [workflows]
    _LOG.debug(hprint.to_str("workflows"))
    # Run.
    for gh_test in gh_tests:
        gh_test += ".yml"
        # gh workflow run fast_tests.yml --ref AmpTask1251_Update_GH_actions_for_amp
        cmd = f"gh workflow run {gh_test} --ref {branch_name}"
        _run(ctx, cmd)
    #
    gh_workflow_list(ctx, branch=branch)


# TODO(gp): Implement this.
# pylint: disable=line-too-long
# @task
# def gh_workflow_passing(ctx, branch="branch", workflows="all"):  # type: ignore
# For each workflow check if the last completed is success or failure
# > gh run list | grep master | grep Fast
# completed       success Fix broken log statement        Fast tests      master  schedule        2m20s   797849342
# completed       success Fix broken log statement        Fast tests      master  push    2m7s    797789759
# completed       success Another speculative fix for break       Fast tests      master  push    1m54s   797556212
# pylint: enable=line-too-long

# #############################################################################


def _get_gh_issue_title(issue_id: int, repo: str) -> str:
    """
    Get the title of a GitHub issue.

    :param repo: `current` refer to the repo where we are, otherwise a repo short
        name (e.g., "amp")
    """
    # Handle the `repo`.
    if repo == "current":
        repo_full_name = git.get_repo_full_name_from_dirname(".")
        repo_short_name = git.get_repo_name(repo_full_name, "full_name")
    else:
        repo_short_name = repo
        repo_full_name = git.get_repo_name(repo_short_name, "short_name")
    _LOG.debug(
        "repo_short_name=%s repo_full_name=%s", repo_short_name, repo_full_name
    )
    # > (export NO_COLOR=1; gh issue view 1251 --json title )
    # {"title":"Update GH actions for amp"}
    dbg.dassert_lte(1, issue_id)
    cmd = f"gh issue view {issue_id} --repo {repo_full_name} --json title"
    _, txt = hsinte.system_to_string(cmd)
    _LOG.debug("txt=\n%s", txt)
    # Parse json.
    dict_ = json.loads(txt)
    _LOG.debug("dict_=\n%s", dict_)
    title = dict_["title"]
    _LOG.debug("title=%s", title)
    # Remove some annoying chars.
    for char in ": + ( ) /".split():
        title = title.replace(char, "")
    # Replace multiple spaces with one.
    title = re.sub(r"\s+", " ", title)
    #
    title = title.replace(" ", "_")
    # Add the `AmpTaskXYZ_...`
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
    _LOG.debug("task_prefix=%s", task_prefix)
    title = "%s%d_%s" % (task_prefix, issue_id, title)
    return title


@task
def gh_issue_title(ctx, issue_id, repo="current"):  # type: ignore
    """
    Print the title that corresponds to the given issue and repo.

    E.g., AmpTask1251_Update_GH_actions_for_amp
    """
    _report_task()
    _ = ctx
    issue_id = int(issue_id)
    print(_get_gh_issue_title(issue_id, repo))


@task
def gh_create_pr(ctx):  # type: ignore
    """
    Create a draft PR for the current branch in the corresponding repo.
    """
    _report_task()
    # TODO(gp): Check whether the PR already exists.
    branch_name = git.get_branch_name()
    repo_full_name = git.get_repo_full_name_from_dirname(".")
    _LOG.info("Creating PR for '%s' in %s", branch_name, repo_full_name)
    cmd = (
        f"gh pr create"
        f" --repo {repo_full_name}"
        " --draft"
        f' --title "{branch_name}"'
        ' --body ""'
    )
    _run(ctx, cmd)
    # TODO(gp): Implement the rest of the flow.
    # Warning: 3 uncommitted changes
    # https://github.com/alphamatic/amp/pull/1298
    # gh pr view https://github.com/alphamatic/amp/pull/1298 --repo alphamatic/amp --web
