"""
Import as:

import helpers.lib_tasks as hlibtask
"""

import datetime
import functools
import glob
import grp
import io
import logging
import os
import pprint
import pwd
import re
import stat
import sys
from typing import Any, Dict, List, Match, Optional, Tuple, Union

import tqdm
import yaml
from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hintrospection as hintros
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hversion as hversio
import helpers.hsecrets as hsecret
import helpers.hversion as hversio
import helpers.hsystem as hsystem
import repo_config as rconf

# Import this way to avoid complexity in propagating the refactoring in all
# the repos downstream.
from helpers.lib_tasks_find import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_gh import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_git import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_integrate import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_lint import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_pytest import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import

_LOG = logging.getLogger(__name__)


# Conventions around `pyinvoke`:
# - `pyinvoke` uses introspection to infer properties of a task, but doesn't
#   support many Python3 features (see https://github.com/pyinvoke/invoke/issues/357)
# - Don't use type hints in `@tasks`
#   - we use `# ignore: type` to avoid mypy complaints
# - Minimize the code in `@tasks` calling other functions to use Python3 features
# - Use `""` as default instead None since `pyinvoke` can only infer a single type


# #############################################################################
# Default params.
# #############################################################################

# This is used to inject the default params.
# TODO(gp): Using a singleton here is not elegant but simple.
_DEFAULT_PARAMS = {
    "BASE_IMAGE": rconf.get_docker_base_image_name(),
    "AM_ECR_BASE_PATH": os.environ["AM_ECR_BASE_PATH"]
}

def set_default_params(params: Dict[str, Any]) -> None:
    global _DEFAULT_PARAMS
    _DEFAULT_PARAMS = params
    _LOG.debug("Assigning:\n%s", pprint.pformat(params))


def has_default_param(key: str) -> bool:
    hdbg.dassert_isinstance(key, str)
    return key in _DEFAULT_PARAMS


def get_default_param(key: str, *, override_value: Any = None) -> Any:
    """
    Return the value from the default parameters dictionary, optionally
    overriding it.
    """
    hdbg.dassert_isinstance(key, str)
    value = None
    if has_default_param(key):
        value = _DEFAULT_PARAMS[key]
    if override_value:
        _LOG.info("Overriding value %s with %s", value, override_value)
        value = override_value
    hdbg.dassert_is_not(
        value, None, "key='%s' not defined from %s", key, _DEFAULT_PARAMS
    )
    return value


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
    hdbg.init_logger(verbosity=logging.DEBUG)
else:
    hdbg.init_logger(verbosity=logging.INFO)


# NOTE: We need to use a `# type: ignore` for all the @task functions because
# pyinvoke infers the argument type from the code and mypy annotations confuse
# it (see https://github.com/pyinvoke/invoke/issues/357).

# In the following, when using `lru_cache`, we use functions from `hsyste`
# instead of `ctx.run()` since otherwise `lru_cache` would cache `ctx`.

# We prefer not to cache functions running `git` to avoid stale values if we
# call git (e.g., if we cache Git hash and then we do a `git pull`).

# pyinvoke `ctx.run()` is useful for unit testing, since it allows to:
# - mock the result of a system call
# - register the issued command line (to create the expected outcome of a test)
# On the other side `system_interaction.py` contains many utilities that make
# it easy to interact with the system.
# Once AmpPart1347 is implemented we can replace all the `ctx.run()` with calls
# to `system_interaction.py`.


_WAS_FIRST_CALL_DONE = False


def _report_task(txt: str = "", container_dir_name: str = ".") -> None:
    # On the first invocation report the version.
    global _WAS_FIRST_CALL_DONE
    if not _WAS_FIRST_CALL_DONE:
        _WAS_FIRST_CALL_DONE = True
        hversio.check_version(container_dir_name)
    # Print the name of the function.
    func_name = hintros.get_function_name(count=1)
    msg = f"## {func_name}: {txt}"
    print(hprint.color_highlight(msg, color="purple"))


# TODO(gp): Move this to helpers.system_interaction and allow to add the switch
#  globally.
def _to_single_line_cmd(cmd: Union[str, List[str]]) -> str:
    """
    Convert a multiline command (as a string or list of strings) into a single
    line.

    E.g., convert
        ```
        IMAGE=.../amp:dev \
            docker-compose \
            --file devops/compose/docker-compose.yml \
            --file devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env
        ```
    into
        ```
        IMAGE=.../amp:dev docker-compose --file ...
        ```
    """
    if isinstance(cmd, list):
        cmd = " ".join(cmd)
    hdbg.dassert_isinstance(cmd, str)
    cmd = cmd.rstrip().lstrip()
    # Remove `\` at the end of the line.
    cmd = re.sub(r" \\\s*$", " ", cmd, flags=re.MULTILINE)
    # Use a single space between words in the command.
    # TODO(gp): This is a bit dangerous if there are multiple spaces in a string
    #  that for some reason are meaningful.
    cmd = " ".join(cmd.split())
    return cmd


# TODO(Grisha): make it public #755.
def _to_multi_line_cmd(docker_cmd_: List[str]) -> str:
    r"""
    Convert a command encoded as a list of strings into a single command
    separated by `\`.

    E.g., convert
    ```
        ['IMAGE=*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev',
            '\n        docker-compose',
            '\n        --file amp/devops/compose/docker-compose.yml',
            '\n        --file amp/devops/compose/docker-compose_as_submodule.yml',
            '\n        --env-file devops/env/default.env']
        ```
    into
        ```
        IMAGE=*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev \
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
        hdbg.dassert(not dc.endswith("\\"), "dc='%s'", dc)
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


# TODO(gp): Pass through command line using a global switch or an env var.
use_one_line_cmd = False


# TODO(Grisha): make it public #755.
def _run(
    ctx: Any,
    cmd: str,
    *args: Any,
    dry_run: bool = False,
    use_system: bool = False,
    print_cmd: bool = False,
    **ctx_run_kwargs: Any,
) -> Optional[int]:
    _LOG.debug(hprint.to_str("cmd dry_run"))
    if use_one_line_cmd:
        cmd = _to_single_line_cmd(cmd)
    _LOG.debug("cmd=%s", cmd)
    if dry_run:
        print(f"Dry-run: > {cmd}")
        _LOG.warning("Skipping execution")
        res = None
    else:
        if print_cmd:
            print(f"> {cmd}")
        if use_system:
            # TODO(gp): Consider using only `hsystem.system()` since it's more
            # reliable.
            res = hsystem.system(cmd, suppress_output=False)
        else:
            result = ctx.run(cmd, *args, **ctx_run_kwargs)
            res = result.return_code
    return res


# TODO(gp): -> system_interaction.py ?
def _to_pbcopy(txt: str, pbcopy: bool) -> None:
    """
    Save the content of txt in the system clipboard.
    """
    txt = txt.rstrip("\n")
    if not pbcopy:
        print(txt)
        return
    if not txt:
        print("Nothing to copy")
        return
    if hsystem.is_running_on_macos():
        # -n = no new line
        cmd = f"echo -n '{txt}' | pbcopy"
        hsystem.system(cmd)
        print(f"\n# Copied to system clipboard:\n{txt}")
    else:
        _LOG.warning("pbcopy works only on macOS")
        print(txt)


# TODO(gp): We should factor out the meaning of the params in a string and add it
#  to all the tasks' help.
def _get_files_to_process(
    modified: bool,
    branch: bool,
    last_commit: bool,
    # TODO(gp): Pass abs_dir, instead of `all_` and remove the calls from the
    # outer clients.
    all_: bool,
    files_from_user: str,
    mutually_exclusive: bool,
    remove_dirs: bool,
) -> List[str]:
    """
    Get a list of files to process.

    The files are selected based on the switches:
    - `branch`: changed in the branch
    - `modified`: changed in the client (both staged and modified)
    - `last_commit`: part of the previous commit
    - `all`: all the files in the repo
    - `files_from_user`: passed by the user

    :param modified: return files modified in the client (i.e., changed with
        respect to HEAD)
    :param branch: return files modified with respect to the branch point
    :param last_commit: return files part of the previous commit
    :param all: return all repo files
    :param files_from_user: return files passed to this function
    :param mutually_exclusive: ensure that all options are mutually exclusive
    :param remove_dirs: whether directories should be processed
    :return: paths to process
    """
    _LOG.debug(
        hprint.to_str(
            "modified branch last_commit all_ files_from_user "
            "mutually_exclusive remove_dirs"
        )
    )
    if mutually_exclusive:
        # All the options are mutually exclusive.
        hdbg.dassert_eq(
            int(modified)
            + int(branch)
            + int(last_commit)
            + int(all_)
            + int(len(files_from_user) > 0),
            1,
            msg="Specify only one among --modified, --branch, --last-commit, "
            "--all_files, and --files",
        )
    else:
        # We filter the files passed from the user through other the options,
        # so only the filtering options need to be mutually exclusive.
        hdbg.dassert_eq(
            int(modified) + int(branch) + int(last_commit) + int(all_),
            1,
            msg="Specify only one among --modified, --branch, --last-commit",
        )
    dir_name = "."
    if modified:
        files = hgit.get_modified_files(dir_name)
    elif branch:
        files = hgit.get_modified_files_in_branch("master", dir_name)
    elif last_commit:
        files = hgit.get_previous_committed_files(dir_name)
    elif all_:
        pattern = "*"
        only_files = True
        use_relative_paths = True
        files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
    if files_from_user:
        # If files were passed, filter out non-existent paths.
        files = _filter_existing_paths(files_from_user.split())
    # Convert into a list.
    hdbg.dassert_isinstance(files, list)
    files_to_process = [f for f in files if f != ""]
    # We need to remove `amp` to avoid copying the entire tree.
    files_to_process = [f for f in files_to_process if f != "amp"]
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Remove dirs, if needed.
    if remove_dirs:
        files_to_process = hsystem.remove_dirs(files_to_process)
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Ensure that there are files to process.
    if not files_to_process:
        _LOG.warning("No files were selected")
    return files_to_process


def _filter_existing_paths(paths_from_user: List[str]) -> List[str]:
    """
    Filter out the paths to non-existent files.

    :param paths_from_user: paths passed by user
    :return: existing paths
    """
    paths = []
    for user_path in paths_from_user:
        if user_path.endswith("/*"):
            # Get the files according to the "*" pattern.
            dir_files = glob.glob(user_path)
            if dir_files:
                # Check whether the pattern matches files.
                paths.extend(dir_files)
            else:
                _LOG.error(
                    (
                        "'%s' pattern doesn't match any files: "
                        "the directory is empty or path does not exist"
                    ),
                    user_path,
                )
        elif os.path.exists(user_path):
            paths.append(user_path)
        else:
            _LOG.error("'%s' does not exist", user_path)
    return paths


# Copied from helpers.datetime_ to avoid dependency from pandas.


def _get_ET_timestamp() -> str:
    # The timezone depends on how the shell is configured.
    timestamp = datetime.datetime.now()
    return timestamp.strftime("%Y%m%d_%H%M%S")


# End copy.

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
    var_names = "AM_ECR_BASE_PATH BASE_IMAGE".split()
    for v in var_names:
        print(f"{v}={get_default_param(v)}")


@task
def print_tasks(ctx, as_code=False):  # type: ignore
    """
    Print all the available tasks in `lib_tasks.py`.

    These tasks might be exposed or not by different.

    :param as_code: print as python code so that it can be embed in a
        `from helpers.lib_tasks import ...`
    """
    _report_task()
    _ = ctx
    func_names = []
    lib_tasks_file_name = os.path.join(
        hgit.get_amp_abs_path(), "helpers/lib_tasks.py"
    )
    hdbg.dassert_file_exists(lib_tasks_file_name)
    # TODO(gp): Use __file__ instead of hardwiring the file.
    cmd = rf'\grep "^@task" -A 1 {lib_tasks_file_name} | grep def'
    # def print_setup(ctx):  # type: ignore
    # def git_pull(ctx):  # type: ignore
    # def git_fetch_master(ctx):  # type: ignore
    _, txt = hsystem.system_to_string(cmd)
    for line in txt.split("\n"):
        _LOG.debug("line=%s", line)
        m = re.match(r"^def\s+(\S+)\(", line)
        if m:
            func_name = m.group(1)
            _LOG.debug("  -> %s", func_name)
            func_names.append(func_name)
    func_names = sorted(func_names)
    if as_code:
        print("\n".join([f"{fn}," for fn in func_names]))
    else:
        print("\n".join(func_names))


@task
def print_env(ctx):  # type: ignore
    """
    Print the repo configuration.
    """
    _ = ctx
    print(henv.env_to_str())


# #############################################################################
# Basic Docker commands.
# #############################################################################

# TODO(gp): @all move to lib_tasks_docker.py


def _get_docker_exec(sudo: bool) -> str:
    docker_exec = "docker"
    if sudo:
        docker_exec = "sudo " + docker_exec
    return docker_exec


@task
def docker_images_ls_repo(ctx, sudo=False):  # type: ignore
    """
    List images in the logged in repo_short_name.
    """
    _report_task()
    docker_login(ctx)
    ecr_base_path = get_default_param("AM_ECR_BASE_PATH")
    docker_exec = _get_docker_exec(sudo)
    _run(ctx, f"{docker_exec} image ls {ecr_base_path}")


@task
def docker_ps(ctx, sudo=False):  # type: ignore
    # pylint: disable=line-too-long
    """
    List all the running containers.

    ```
    > docker_ps
    CONTAINER ID  user  IMAGE                    COMMAND                    CREATED        STATUS        PORTS  service
    2ece37303ec9  gp    *****....:latest  "./docker_build/entry.sh"  5 seconds ago  Up 4 seconds         user_space
    ```
    """
    _report_task()
    # pylint: enable=line-too-long
    fmt = (
        r"""table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}"""
        + r"\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}"
        + r'\t{{.Label "com.docker.compose.service"}}'
    )
    docker_exec = _get_docker_exec(sudo)
    cmd = f"{docker_exec} ps --format='{fmt}'"
    cmd = _to_single_line_cmd(cmd)
    _run(ctx, cmd)


def _get_last_container_id(sudo: bool) -> str:
    docker_exec = _get_docker_exec(sudo)
    # Get the last started container.
    cmd = f"{docker_exec} ps -l | grep -v 'CONTAINER ID'"
    # CONTAINER ID   IMAGE          COMMAND                  CREATED
    # 90897241b31a   eeb33fe1880a   "/bin/sh -c '/bin/bash ...
    _, txt = hsystem.system_to_one_line(cmd)
    # Parse the output: there should be at least one line.
    hdbg.dassert_lte(1, len(txt.split(" ")), "Invalid output='%s'", txt)
    container_id: str = txt.split(" ")[0]
    return container_id


@task
def docker_stats(  # type: ignore
    ctx,
    all=False,  # pylint: disable=redefined-builtin
    sudo=False,
):
    # pylint: disable=line-too-long
    """
    Report last started Docker container stats, e.g., CPU, RAM.

    ```
    > docker_stats
    CONTAINER ID  NAME                   CPU %  MEM USAGE / LIMIT     MEM %  NET I/O         BLOCK I/O        PIDS
    2ece37303ec9  ..._user_space_run_30  0.00%  15.74MiB / 31.07GiB   0.05%  351kB / 6.27kB  34.2MB / 12.3kB  4
    ```

    :param all: report stats for all the containers
    """
    # pylint: enable=line-too-long
    _report_task(txt=hprint.to_str("all"))
    _ = ctx
    fmt = (
        r"table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
        + r"\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}"
    )
    docker_exec = _get_docker_exec(sudo)
    cmd = f"{docker_exec} stats --no-stream --format='{fmt}'"
    _, txt = hsystem.system_to_string(cmd)
    if all:
        output = txt
    else:
        # Get the id of the last started container.
        container_id = _get_last_container_id(sudo)
        print(f"Last container id={container_id}")
        # Parse the output looking for the given container.
        txt = txt.split("\n")
        output = []
        # Save the header.
        output.append(txt[0])
        for line in txt[1:]:
            if line.startswith(container_id):
                output.append(line)
        # There should be at most two rows: the header and the one corresponding to
        # the container.
        hdbg.dassert_lte(
            len(output), 2, "Invalid output='%s' for '%s'", output, txt
        )
        output = "\n".join(output)
    print(output)


@task
def docker_kill(  # type: ignore
    ctx,
    all=False,  # pylint: disable=redefined-builtin
    sudo=False,
):
    """
    Kill the last Docker container started.

    :param all: kill all the containers (be careful!)
    :param sudo: use sudo for the Docker commands
    """
    _report_task(txt=hprint.to_str("all"))
    docker_exec = _get_docker_exec(sudo)
    # Last container.
    opts = "-l"
    if all:
        _LOG.warning("Killing all the containers")
        # TODO(gp): Ask if we are sure and add a --just-do-it option.
        opts = "-a"
    # Print the containers that will be terminated.
    cmd = f"{docker_exec} ps {opts}"
    _run(ctx, cmd)
    # Kill.
    cmd = f"{docker_exec} rm -f $({docker_exec} ps {opts} -q)"
    _run(ctx, cmd)


# docker system prune
# docker container ps -f "status=exited"
# docker container rm $(docker container ps -f "status=exited" -q)
# docker rmi $(docker images --filter="dangling=true" -q)

# pylint: disable=line-too-long
# Remove the images with hash
# > docker image ls
# REPOSITORY                                        TAG                                        IMAGE ID       CREATED         SIZE
# *****.dkr.ecr.us-east-2.amazonaws.com/im          07aea615a2aa9290f7362e99e1cc908876700821   d0889bf972bf   6 minutes ago   684MB
# *****.dkr.ecr.us-east-2.amazonaws.com/im          rc                                         d0889bf972bf   6 minutes ago   684MB
# python                                            3.7-slim-buster                            e7d86653f62f   14 hours ago    113MB
# *****.dkr.ecr.us-east-1.amazonaws.com/dev_tools   ce789e4718175fcdf6e4857581fef1c2a5ee81f3   2f64ade2c048   14 hours ago    2.02GB
# *****.dkr.ecr.us-east-1.amazonaws.com/dev_tools   local                                      2f64ade2c048   14 hours ago    2.02GB
# *****.dkr.ecr.us-east-1.amazonaws.com/dev_tools   d401a2a0bef90b9f047c65f8adb53b28ba05d536   1b11bf234c7f   15 hours ago    2.02GB
# *****.dkr.ecr.us-east-1.amazonaws.com/dev_tools   52ccd63edbc90020f450c074b7c7088a1806c5ac   90b70a55c367   15 hours ago    1.95GB
# *****.dkr.ecr.us-east-1.amazonaws.com/dev_tools   2995608a7d91157fc1a820869a6d18f018c3c598   0cb3858e85c6   15 hours ago    2.01GB
# *****.dkr.ecr.us-east-1.amazonaws.com/amp         415376d58001e804e840bf3907293736ad62b232   e6ea837ab97f   18 hours ago    1.65GB
# *****.dkr.ecr.us-east-1.amazonaws.com/amp         dev                                        e6ea837ab97f   18 hours ago    1.65GB
# *****.dkr.ecr.us-east-1.amazonaws.com/amp         local                                      e6ea837ab97f   18 hours ago    1.65GB
# *****.dkr.ecr.us-east-1.amazonaws.com/amp         9586cc2de70a4075b9fdcdb900476f8a0f324e3e   c75d2447da79   18 hours ago    1.65GB
# pylint: enable=line-too-long


# #############################################################################
# Docker development.
# #############################################################################

# TODO(gp): We might want to organize the code in a base class using a Command
# pattern, so that it's easier to generalize the code for multiple repos.
#
# class DockerCommand:
#   def pull():
#     ...
#   def cmd():
#     ...
#
# For now we pass the customizable part through the default params.


def _docker_pull(
    ctx: Any, base_image: str, stage: str, version: Optional[str]
) -> None:
    """
    Pull images from the registry.
    """
    docker_login(ctx)
    #
    image = get_image(base_image, stage, version)
    _LOG.info("image='%s'", image)
    _dassert_is_image_name_valid(image)
    cmd = f"docker pull {image}"
    _run(ctx, cmd, pty=True)


@task
def docker_pull(ctx, stage="dev", version=None):  # type: ignore
    """
    Pull latest dev image corresponding to the current repo from the registry.
    """
    _report_task()
    #
    base_image = ""
    _docker_pull(ctx, base_image, stage, version)


@task
def docker_pull_dev_tools(ctx, stage="prod", version=None):  # type: ignore
    """
    Pull latest prod image of `dev_tools` from the registry.
    """
    _report_task()
    #
    base_image = get_default_param("AM_ECR_BASE_PATH") + "/dev_tools"
    _docker_pull(ctx, base_image, stage, version)


@functools.lru_cache()
def _get_aws_cli_version() -> int:
    # > aws --version
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    # aws-cli/1.20.1 Python/3.9.5 Darwin/19.6.0 botocore/1.20.106
    cmd = "aws --version"
    res = hsystem.system_to_one_line(cmd)[1]
    # Parse the output.
    m = re.match(r"aws-cli/((\d+)\.\d+\.\d+)\s", res)
    hdbg.dassert(m, "Can't parse '%s'", res)
    m: Match[Any]
    version = m.group(1)
    _LOG.debug("version=%s", version)
    major_version = int(m.group(2))
    _LOG.debug("major_version=%s", major_version)
    return major_version


@task
def docker_login(ctx):  # type: ignore
    """
    Log in the AM Docker repo_short_name on AWS.
    """
    _report_task()
    if hsystem.is_inside_ci():
        _LOG.warning("Running inside GitHub Action: skipping `docker_login`")
        return
    major_version = _get_aws_cli_version()
    # docker login \
    #   -u AWS \
    #   -p eyJ... \
    #   -e none \
    #   https://*****.dkr.ecr.us-east-1.amazonaws.com
    # TODO(gp): We should get this programmatically from ~/aws/.credentials
    region = "us-east-1"
    if major_version == 1:
        cmd = f"eval $(aws ecr get-login --profile am --no-include-email --region {region})"
    else:
        ecr_base_path = get_default_param("AM_ECR_BASE_PATH")
        cmd = (
            f"docker login -u AWS -p $(aws ecr get-login --region {region}) "
            + f"https://{ecr_base_path}"
        )
    # cmd = ("aws ecr get-login-password" +
    #       " | docker login --username AWS --password-stdin "
    # TODO(Grisha): fix properly. We pass `ctx` despite the fact that we do not
    #  need it with `use_system=True`, but w/o `ctx` invoke tasks (i.e. ones
    #  with `@task` decorator) do not work.
    _run(ctx, cmd, use_system=True)


# ////////////////////////////////////////////////////////////////////////////////
# Compose files.
# ////////////////////////////////////////////////////////////////////////////////

# TODO(gp): All this code can become `DockerComposeFileGenerator`.

# There are several combinations to consider:
# - whether the Docker host can run with / without privileged mode
# - amp as submodule / as supermodule
# - different supermodules for amp

# TODO(gp): use_privileged_mode -> use_docker_privileged_mode
#  use_sibling_container -> use_docker_containers_containers


def _get_linter_service() -> str:
    """
    Get the linter service specification for the `docker-compose.yml` file.

    :return: the text of the linter service specification
    """
    superproject_path, submodule_path = hgit.get_path_from_supermodule()
    if superproject_path:
        # We are running in a Git submodule.
        work_dir = f"/src/{submodule_path}"
        repo_root = superproject_path
    else:
        work_dir = "/src"
        repo_root = os.getcwd()
    linter_spec_txt = f"""
    linter:
      extends:
        base_app
      volumes:
        - {repo_root}:/src
      working_dir: {work_dir}
      environment:
        - MYPYPATH
        # Use the `repo_config.py` inside the dev_tools container instead of
        # the one in the calling repo.
        - AM_REPO_CONFIG_PATH=/app/repo_config.py
    """
    return linter_spec_txt


def _generate_docker_compose_file(
    use_privileged_mode: bool,
    use_sibling_container: bool,
    shared_data_dirs: Optional[Dict[str, str]],
    mount_as_submodule: bool,
    use_network_mode_host: bool,
    use_main_network: bool,
    file_name: Optional[str],
) -> str:
    """
    Generate `docker-compose.yml` file and save it.

    :param shared_data_dirs: data directory in the host filesystem to mount
        inside the container. `None` means no dir sharing
    :param use_main_network: use `main_network` as default network
    """
    _LOG.debug(
        hprint.to_str(
            "use_privileged_mode "
            "use_sibling_container "
            "shared_data_dirs "
            "mount_as_submodule "
            "use_network_mode_host "
            "use_main_network "
            "file_name "
        )
    )
    txt = []

    def append(txt_tmp: str, indent_level: int) -> None:
        # txt_tmp = txt_tmp.rstrip("\n").lstrip("\n")
        txt_tmp = hprint.dedent(txt_tmp, remove_empty_leading_trailing_lines=True)
        num_spaces = 2 * indent_level
        txt_tmp = hprint.indent(txt_tmp, num_spaces=num_spaces)
        txt.append(txt_tmp)

    # We could pass the env var directly, like:
    # ```
    # - AM_ENABLE_DIND=$AM_ENABLE_DIND
    # ```
    # but we prefer to inline it.
    if use_privileged_mode:
        am_enable_dind = 1
    else:
        am_enable_dind = 0
    # sysname='Linux'
    # nodename='cf-spm-dev4'
    # release='3.10.0-1160.53.1.el7.x86_64'
    # version='#1 SMP Fri Jan 14 13:59:45 UTC 2022'
    # machine='x86_64'
    am_host_os_name = os.uname()[0]
    am_host_name = os.uname()[1]
    # We could do the same also with IMAGE for symmetry.
    # Keep the env vars in sync with what we print in `henv.get_env_vars()`.
    txt_tmp = f"""
    version: '3'

    services:
      base_app:
        cap_add:
          - SYS_ADMIN
        environment:
          - AM_AWS_ACCESS_KEY_ID=$AM_AWS_ACCESS_KEY_ID
          - AM_AWS_DEFAULT_REGION=$AM_AWS_DEFAULT_REGION
          - AM_AWS_PROFILE=$AM_AWS_PROFILE
          - AM_AWS_S3_BUCKET=$AM_AWS_S3_BUCKET
          - AM_AWS_SECRET_ACCESS_KEY=$AM_AWS_SECRET_ACCESS_KEY
          - AM_ECR_BASE_PATH=$AM_ECR_BASE_PATH
          - AM_ENABLE_DIND={am_enable_dind}
          - AM_FORCE_TEST_FAIL=$AM_FORCE_TEST_FAIL
          - AM_HOST_NAME={am_host_name}
          - AM_HOST_OS_NAME={am_host_os_name}
          - AM_REPO_CONFIG_CHECK=True
          # Use inferred path for `repo_config.py`.
          - AM_REPO_CONFIG_PATH=
          - AM_PUBLISH_NOTEBOOK_LOCAL_PATH=$AM_PUBLISH_NOTEBOOK_LOCAL_PATH
          - AM_TELEGRAM_TOKEN=$AM_TELEGRAM_TOKEN
          - CK_AWS_ACCESS_KEY_ID=$CK_AWS_ACCESS_KEY_ID
          - CK_AWS_DEFAULT_REGION=$CK_AWS_DEFAULT_REGION
          - CK_AWS_PROFILE=$CK_AWS_PROFILE
          - CK_AWS_S3_BUCKET=$CK_AWS_S3_BUCKET
          - CK_AWS_SECRET_ACCESS_KEY=$CK_AWS_SECRET_ACCESS_KEY
          # - CK_ECR_BASE_PATH=$CK_ECR_BASE_PATH
          # - CK_ENABLE_DIND=
          # - CK_FORCE_TEST_FAIL=$CK_FORCE_TEST_FAIL
          # - CK_HOST_NAME=
          # - CK_HOST_OS_NAME=
          # - CK_PUBLISH_NOTEBOOK_LOCAL_PATH=$CK_PUBLISH_NOTEBOOK_LOCAL_PATH
          - CK_TELEGRAM_TOKEN=$CK_TELEGRAM_TOKEN
          - GH_ACTION_ACCESS_TOKEN=$GH_ACTION_ACCESS_TOKEN
          # This env var is used by GH Action to signal that we are inside the CI.
          - CI=$CI
        image: ${{IMAGE}}
    """
    indent_level = 0
    append(txt_tmp, indent_level)
    #
    if use_privileged_mode:
        txt_tmp = """
        # This is needed:
        # - for Docker-in-docker (dind)
        # - to mount fstabs
        privileged: true
        """
        # This is at the level of `services.app`.
        indent_level = 2
        append(txt_tmp, indent_level)
    #
    if True:
        txt_tmp = """
        restart: "no"
        volumes:
          # TODO(gp): We should pass the value of $HOME from dev.Dockerfile to here.
          # E.g., we might define $HOME in the env file.
          - ~/.aws:/home/.aws
          - ~/.config/gspread_pandas/:/home/.config/gspread_pandas/
          - ~/.config/gh:/home/.config/gh
        """
        # This is at the level of `services.app`.
        indent_level = 2
        append(txt_tmp, indent_level)
        # Mount shared dirs.
        if shared_data_dirs is not None:
            hdbg.dassert_lt(0, len(shared_data_dirs))
            #
            txt_tmp = "# Shared data directories."
            # This is at the level of `services.app.volumes`.
            indent_level = 3
            append(txt_tmp, indent_level)
            # Mount all dirs that are specified.
            for key, value in shared_data_dirs.items():
                txt_tmp = f"""
                - {key}:{value}
                """
                append(txt_tmp, indent_level)
    #
    if False:
        txt_tmp = """
        # No need to mount file systems.
        - ../docker_build/fstab:/etc/fstab
        """
        # This is at the level of `services.app.volumes`.
        indent_level = 3
        append(txt_tmp, indent_level)
    #
    if use_sibling_container:
        txt_tmp = """
        # Use sibling-container approach.
        - /var/run/docker.sock:/var/run/docker.sock
        """
        # This is at the level of `services.app.volumes`.
        indent_level = 3
        append(txt_tmp, indent_level)
    #
    if False:
        txt_tmp = """
        deploy:
          resources:
            limits:
              # This should be passed from command line depending on how much
              # memory is available.
              memory: 60G
        """
        # This is at the level of `services/app`.
        indent_level = 2
        append(txt_tmp, indent_level)
    #
    if use_network_mode_host:
        txt_tmp = """
        # Default network mode set to host so we can reach e.g.
        # a database container pointing to localhost:5432.
        # In tests we use dind so we need set back to the default "bridge".
        # See CmTask988 and https://stackoverflow.com/questions/24319662
        network_mode: ${NETWORK_MODE:-host}
        """
        # This is at the level of `services/app`.
        indent_level = 2
        append(txt_tmp, indent_level)
    #
    if mount_as_submodule:
        txt_tmp = """
        # Mount `amp` when it is used as submodule. In this case we need to
        # mount the super project in the container (to make git work with the
        # supermodule) and then change dir to `amp`.
        app:
          extends:
            base_app
          volumes:
            # Move one dir up to include the entire git repo (see AmpTask1017).
            - ../../../:/app
          # Move one dir down to include the entire git repo (see AmpTask1017).
          working_dir: /app/amp
        """
    else:
        txt_tmp = """
        # Mount `amp` when it is used as supermodule.
        app:
          extends:
            base_app
          volumes:
            - ../../:/app
        """
    # This is at the level of `services`.
    indent_level = 1
    append(txt_tmp, indent_level)
    #
    if True:
        # Specify the linter service.
        txt_tmp = _get_linter_service()
        # Append at the level of `services`.
        indent_level = 1
        append(txt_tmp, indent_level)
    #
    if True:
        # For Jupyter server we cannot use "host" network_mode because
        # it is incompatible with the port bindings.
        txt_tmp = """
        jupyter_server:
          command: devops/docker_run/run_jupyter_server.sh
          environment:
            - PORT=${PORT}
          extends:
            app
          network_mode: ${NETWORK_MODE:-bridge}
          ports:
            # TODO(gp): Rename `AM_PORT`.
            - "${PORT}:${PORT}"

        # TODO(gp): For some reason the following doesn't work.
        #  jupyter_server_test:
        #    command: jupyter notebook -h 2>&1 >/dev/null
        #    extends:
        #      jupyter_server

        jupyter_server_test:
          command: jupyter notebook -h 2>&1 >/dev/null
          environment:
            - PORT=${PORT}
          extends:
            app
          network_mode: ${NETWORK_MODE:-bridge}
          ports:
            - "${PORT}:${PORT}"
        """
        # This is inside `services`.
        indent_level = 1
        append(txt_tmp, indent_level)
    #
    if use_main_network:
        txt_tmp = """
        networks:
          default:
            name: main_network
        """
        # This is at the level of `services`.
        indent_level = 0
        append(txt_tmp, indent_level)
    # Save file.
    txt_str: str = "\n".join(txt)
    if file_name:
        hio.to_file(file_name, txt_str)
    # Sanity check of the YAML file.
    stream = io.StringIO(txt_str)
    _ = yaml.safe_load(stream)
    return txt_str


def get_base_docker_compose_path() -> str:
    """
    Return the absolute path to the Docker compose file.

    E.g., `devops/compose/docker-compose.yml`.
    """
    # Add the default path.
    dir_name = "devops/compose"
    # TODO(gp): Factor out the piece below.
    docker_compose_path = "docker-compose.yml"
    docker_compose_path = os.path.join(dir_name, docker_compose_path)
    docker_compose_path = os.path.abspath(docker_compose_path)
    return docker_compose_path


def _get_docker_compose_files(
    generate_docker_compose_file: bool,
    service_name: str,
    extra_docker_compose_files: Optional[List[str]],
) -> List[str]:
    """
    Generate the Docker compose file and return the list of Docker compose
    paths.

    :return: list of the Docker compose paths
    """
    docker_compose_files = []
    # Get the repo short name (e.g., amp).
    dir_name = hgit.get_repo_full_name_from_dirname(".", include_host_name=False)
    repo_short_name = hgit.get_repo_name(dir_name, in_mode="full_name")
    _LOG.debug("repo_short_name=%s", repo_short_name)
    # Check submodule status, if needed.
    mount_as_submodule = False
    if repo_short_name in ("amp", "cm"):
        # Check if `amp` is a submodule.
        path, _ = hgit.get_path_from_supermodule()
        if path != "":
            _LOG.warning("amp is a submodule")
            mount_as_submodule = True
    # Write Docker compose file.
    file_name = get_base_docker_compose_path()
    if service_name == "linter":
        # Since we are running the prod `dev_tools` container we need to use the
        # settings from the `repo_config` from that container, and not the settings
        # launch the container corresponding to this repo.
        enable_privileged_mode = False
        use_docker_sibling_containers = False
        get_shared_data_dirs = None
        use_docker_network_mode_host = False
        use_main_network = False
    else:
        # Use the settings from the `repo_config` corresponding to this container.
        enable_privileged_mode = hgit.execute_repo_config_code(
            "enable_privileged_mode()"
        )
        use_docker_sibling_containers = hgit.execute_repo_config_code(
            "use_docker_sibling_containers()"
        )
        get_shared_data_dirs = hgit.execute_repo_config_code(
            "get_shared_data_dirs()"
        )
        use_docker_network_mode_host = hgit.execute_repo_config_code(
            "use_docker_network_mode_host()"
        )
        use_main_network = hgit.execute_repo_config_code("use_main_network()")
    #
    if generate_docker_compose_file:
        _generate_docker_compose_file(
            enable_privileged_mode,
            use_docker_sibling_containers,
            get_shared_data_dirs,
            mount_as_submodule,
            use_docker_network_mode_host,
            use_main_network,
            file_name,
        )
    else:
        _LOG.warning("Skipping generating Docker compose file '%s'", file_name)
    docker_compose_files.append(file_name)
    # Add the compose files from command line.
    if extra_docker_compose_files:
        hdbg.dassert_isinstance(extra_docker_compose_files, list)
        docker_compose_files.extend(extra_docker_compose_files)
    # Add the compose files from the global params.
    key = "DOCKER_COMPOSE_FILES"
    if has_default_param(key):
        docker_compose_files.append(get_default_param(key))
    #
    _LOG.debug(hprint.to_str("docker_compose_files"))
    for docker_compose in docker_compose_files:
        hdbg.dassert_path_exists(docker_compose)
    return docker_compose_files


# ////////////////////////////////////////////////////////////////////////////////
# Version.
# ////////////////////////////////////////////////////////////////////////////////


_IMAGE_VERSION_RE = r"\d+\.\d+\.\d+"


def _dassert_is_version_valid(version: str) -> None:
    """
    Check that the version is valid, i.e. looks like `1.0.0`.
    """
    hdbg.dassert_isinstance(version, str)
    hdbg.dassert_ne(version, "")
    regex = rf"^({_IMAGE_VERSION_RE})$"
    _LOG.debug("Testing with regex='%s'", regex)
    m = re.match(regex, version)
    hdbg.dassert(m, "Invalid version: '%s'", version)


_IMAGE_VERSION_FROM_CHANGELOG = "FROM_CHANGELOG"


def _resolve_version_value(
    version: str,
    *,
    container_dir_name: str = ".",
) -> str:
    """
    Pass a version (e.g., 1.0.0) or a symbolic value (e.g., FROM_CHANGELOG) and
    return the resolved value of the version.
    """
    hdbg.dassert_isinstance(version, str)
    if version == _IMAGE_VERSION_FROM_CHANGELOG:
        version = hversio.get_changelog_version(container_dir_name)
    _dassert_is_version_valid(version)
    return version


def _dassert_is_subsequent_version(
    version: str,
    *,
    container_dir_name: str = ".",
) -> None:
    """
    Check that version is strictly bigger than the current one as specified in
    the changelog.
    """
    if version != _IMAGE_VERSION_FROM_CHANGELOG:
        current_version = hversio.get_changelog_version(container_dir_name)
        hdbg.dassert_lt(current_version, version)


# ////////////////////////////////////////////////////////////////////////////////
# Image.
# ////////////////////////////////////////////////////////////////////////////////


_INTERNET_ADDRESS_RE = r"([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
_IMAGE_BASE_NAME_RE = r"[a-z0-9_-]+"
_IMAGE_USER_RE = r"[a-z0-9_-]+"
# For candidate prod images which have added hash for easy identification.
_IMAGE_HASH_RE = r"[a-z0-9]{9}"
_IMAGE_STAGE_RE = (
    rf"(local(?:-{_IMAGE_USER_RE})?|dev|prod|prod(?:-{_IMAGE_HASH_RE})?)"
)


def _dassert_is_image_name_valid(image: str) -> None:
    """
    Check whether an image name is valid.

    Invariants:
    - Local images contain a user name and a version
      - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local-saggese-1.0.0`
    - `dev` and `prod` images have an instance with the a version and one without
      to indicate the latest
      - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev-1.0.0`
        and `*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev`
    - `prod` candidate image has a 9 character hash identifier from the
        corresponding Git commit
        - E.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:prod-1.0.0-4rf74b83a`

    An image should look like:

    *****.dkr.ecr.us-east-1.amazonaws.com/amp:dev
    *****.dkr.ecr.us-east-1.amazonaws.com/amp:local-saggese-1.0.0
    *****.dkr.ecr.us-east-1.amazonaws.com/amp:dev-1.0.0
    """
    regex = "".join(
        [
            # E.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
            rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_BASE_NAME_RE}",
            # :local-saggese
            rf":{_IMAGE_STAGE_RE}",
            # -1.0.0
            rf"(-{_IMAGE_VERSION_RE})?$",
        ]
    )
    _LOG.debug("Testing with regex='%s'", regex)
    m = re.match(regex, image)
    hdbg.dassert(m, "Invalid image: '%s'", image)


def _dassert_is_base_image_name_valid(base_image: str) -> None:
    """
    Check that the base image is valid, i.e. looks like below.

    *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    regex = rf"^{_INTERNET_ADDRESS_RE}\/{_IMAGE_BASE_NAME_RE}$"
    _LOG.debug("regex=%s", regex)
    m = re.match(regex, base_image)
    hdbg.dassert(m, "Invalid base_image: '%s'", base_image)


def _get_base_image(base_image: str) -> str:
    """
    :return: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    if base_image == "":
        # TODO(gp): Use os.path.join.
        base_image = (
            get_default_param("AM_ECR_BASE_PATH")
            + "/"
            + get_default_param("BASE_IMAGE")
        )
    _dassert_is_base_image_name_valid(base_image)
    return base_image


# This code path through Git tag was discontinued with CmTask746.
# def get_git_tag(
#      version: str,
#  ) -> str:
#      """
#      Return the tag to be used in Git that consists of an image name and
#      version.
#      :param version: e.g., `1.0.0`. If None, the latest version is used
#      :return: e.g., `amp-1.0.0`
#      """
#      hdbg.dassert_is_not(version, None)
#      _dassert_is_version_valid(version)
#      base_image = get_default_param("BASE_IMAGE")
#      tag_name = f"{base_image}-{version}"
#      return tag_name


# TODO(gp): Consider using a token "latest" in version, so that it's always a
#  string and we avoid a special behavior encoded in None.
def get_image(
    base_image: str,
    stage: str,
    version: Optional[str],
) -> str:
    """
    Return the fully qualified image name.

    For local stage, it also appends the user name to the image name.

    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param stage: e.g., `local`, `dev`, `prod`
    :param version: e.g., `1.0.0`, if None empty, the latest version is used
    :return: e.g., `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local` or
        `*****.dkr.ecr.us-east-1.amazonaws.com/amp:local-1.0.0`
    """
    # Docker refers the default image as "latest", although in our stage
    # nomenclature we call it "dev".
    hdbg.dassert_in(stage, "local dev prod".split())
    # Get the base image.
    base_image = _get_base_image(base_image)
    _dassert_is_base_image_name_valid(base_image)
    # Get the full image name.
    image = [base_image]
    # Handle the stage.
    image.append(f":{stage}")
    # User the user name.
    if stage == "local":
        user = hsystem.get_user_name()
        image.append(f"-{user}")
    # Handle the version.
    if version is not None and version != "":
        _dassert_is_version_valid(version)
        image.append(f"-{version}")
    #
    image = "".join(image)
    _dassert_is_image_name_valid(image)
    return image


# ////////////////////////////////////////////////////////////////////////////////
# Misc.
# ////////////////////////////////////////////////////////////////////////////////


def _run_docker_as_user(as_user_from_cmd_line: bool) -> bool:
    as_root = hgit.execute_repo_config_code("run_docker_as_root()")
    as_user = as_user_from_cmd_line
    if as_root:
        as_user = False
    _LOG.debug(
        "as_user_from_cmd_line=%s as_root=%s -> as_user=%s",
        as_user_from_cmd_line,
        as_root,
        as_user,
    )
    return as_user


def _get_container_name(service_name: str) -> str:
    """
    Create a container name based on various information (e.g.,
    `grisha.cmamp.app.cmamp1.20220317_232120`).

    The information used to build a container is:
       - Linux user name
       - Base Docker image name
       - Service name
       - Project directory that was used to start a container
       - Container start timestamp

    :param service_name: `docker-compose` service name, e.g., `app`
    :return: container name
    """
    hdbg.dassert_ne(service_name, "", "You need to specify a service name")
    # Get linux user name.
    linux_user = hsystem.get_user_name()
    # Get dir name.
    project_dir = hgit.get_project_dirname()
    # Get Docker image base name.
    image_name = get_default_param("BASE_IMAGE")
    # Get current timestamp.
    current_timestamp = _get_ET_timestamp()
    # Build container name.
    container_name = f"{linux_user}.{image_name}.{service_name}.{project_dir}.{current_timestamp}"
    _LOG.debug(
        "get_container_name: container_name=%s",
        container_name,
    )
    return container_name


def _get_docker_base_cmd(
    base_image: str,
    stage: str,
    version: str,
    service_name: str,
    generate_docker_compose_file: bool,
    extra_env_vars: Optional[List[str]],
    extra_docker_compose_files: Optional[List[str]],
) -> List[str]:
    r"""
    Get base `docker-compose` command encoded as a list of strings.

    It can be used as a base to build more complex commands, e.g., `run`, `up`, `down`.

    E.g.,
    ```
        ['IMAGE=*****.dkr.ecr.us-east-1.amazonaws.com/amp:dev',
            '\n        docker-compose',
            '\n        --file amp/devops/compose/docker-compose.yml',
            '\n        --file amp/devops/compose/docker-compose_as_submodule.yml',
            '\n        --env-file devops/env/default.env']
    ```
    :param generate_docker_compose_file: whether to generate or reuse the existing
        Docker compose file
    :param extra_env_vars: represent vars to add, e.g., `["PORT=9999", "DRY_RUN=1"]`
    :param extra_docker_compose_files: `docker-compose` override files
    """
    hprint.log(
        _LOG,
        logging.DEBUG,
        "base_image stage version extra_env_vars extra_docker_compose_files",
    )
    docker_cmd_: List[str] = []
    # - Handle the image.
    image = get_image(base_image, stage, version)
    _LOG.debug("base_image=%s stage=%s -> image=%s", base_image, stage, image)
    _dassert_is_image_name_valid(image)
    docker_cmd_.append(f"IMAGE={image}")
    # - Handle extra env vars.
    if extra_env_vars:
        hdbg.dassert_isinstance(extra_env_vars, list)
        for env_var in extra_env_vars:
            docker_cmd_.append(f"{env_var}")
    #
    docker_cmd_.append(
        r"""
        docker-compose"""
    )
    docker_compose_files = _get_docker_compose_files(
        generate_docker_compose_file, service_name, extra_docker_compose_files
    )
    file_opts = " ".join([f"--file {dcf}" for dcf in docker_compose_files])
    _LOG.debug(hprint.to_str("file_opts"))
    # TODO(gp): Use something like `.append(rf"{space}{...}")`
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
    return docker_cmd_


def _get_docker_compose_cmd(
    base_image: str,
    stage: str,
    version: str,
    cmd: str,
    *,
    # TODO(gp): make these params mandatory.
    extra_env_vars: Optional[List[str]] = None,
    extra_docker_compose_files: Optional[List[str]] = None,
    extra_docker_run_opts: Optional[List[str]] = None,
    service_name: str = "app",
    entrypoint: bool = True,
    generate_docker_compose_file: bool = True,
    as_user: bool = True,
    print_docker_config: bool = False,
    use_bash: bool = False,
) -> str:
    """
    Get `docker-compose` run command.

    E.g.,
    ```
    IMAGE=*****..dkr.ecr.us-east-1.amazonaws.com/amp:dev \
        docker-compose \
        --file /amp/devops/compose/docker-compose.yml \
        --env-file devops/env/default.env \
        run \
        --rm \
        --name grisha.cmamp.app.cmamp1.20220317_232120 \
        --user $(id -u):$(id -g) \
        app \
        bash
    ```
    :param cmd: command to run inside Docker container
    :param extra_docker_run_opts: additional `docker-compose` run options
    :param service_name: service to use to run a command
    :param entrypoint: whether to use the `entrypoint` or not
    :param generate_docker_compose_file: generate the Docker compose file or not
    :param as_user: pass the user / group id or not
    :param print_docker_config: print the docker config for debugging purposes
    :param use_bash: run command through a shell
    """
    hprint.log(
        _LOG,
        logging.DEBUG,
        "cmd extra_docker_run_opts service_name "
        "entrypoint as_user print_docker_config use_bash",
    )
    # - Get the base Docker command.
    docker_cmd_ = _get_docker_base_cmd(
        base_image,
        stage,
        version,
        service_name,
        generate_docker_compose_file,
        extra_env_vars,
        extra_docker_compose_files,
    )
    # - Add the `config` command for debugging purposes.
    docker_config_cmd: List[str] = docker_cmd_[:]
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
    # - Add a name to the container.
    container_name = _get_container_name(service_name)
    docker_cmd_.append(
        rf"""
        --name {container_name}"""
    )
    # - Handle the user.
    as_user = _run_docker_as_user(as_user)
    if as_user:
        docker_cmd_.append(
            r"""
        --user $(id -u):$(id -g)"""
        )
    # - Handle the extra docker options.
    if extra_docker_run_opts:
        hdbg.dassert_isinstance(extra_docker_run_opts, list)
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
            if use_bash:
                cmd = f"bash -c '{cmd}'"
            docker_cmd_.append(
                rf"""
        {cmd}"""
            )
    else:
        # No entrypoint.
        docker_cmd_.append(
            rf"""
        --entrypoint bash \
        {service_name}"""
        )
    # Print the config for debugging purpose.
    if print_docker_config:
        docker_config_cmd_as_str = _to_multi_line_cmd(docker_config_cmd)
        _LOG.debug("docker_config_cmd=\n%s", docker_config_cmd_as_str)
        _LOG.debug(
            "docker_config=\n%s",
            hsystem.system_to_string(docker_config_cmd_as_str)[1],
        )
    # Print the config for debugging purpose.
    docker_cmd_ = _to_multi_line_cmd(docker_cmd_)
    return docker_cmd_


def _get_lint_docker_cmd(
    docker_cmd_: str,
    stage: str,
    version: str,
    *,
    entrypoint: bool = True,
) -> str:
    """
    Create a command to run in the Linter service.

    :param docker_cmd_: command to run
    :param stage: the image stage to use
    :return: the full command to run
    """
    # Get an image to run the linter on.
    ecr_base_path = os.environ["AM_ECR_BASE_PATH"]
    linter_image = f"{ecr_base_path}/dev_tools"
    # TODO(Grisha): do we need a version? i.e., we can pass `version` to `lint`
    # and run Linter on the specific version, e.g., `1.1.5`.
    # Execute command line.
    cmd: str = _get_docker_compose_cmd(
        linter_image,
        stage,
        version,
        docker_cmd_,
        entrypoint=entrypoint,
        service_name="linter",
    )
    return cmd


# ////////////////////////////////////////////////////////////////////////////////
# bash and cmd.
# ////////////////////////////////////////////////////////////////////////////////


def _docker_cmd(
    ctx: Any,
    docker_cmd_: str,
    **ctx_run_kwargs: Any,
) -> Optional[int]:
    """
    Execute a docker command printing the command.

    :param kwargs: kwargs for `ctx.run`
    """
    _LOG.info("Pulling the latest version of Docker")
    docker_pull(ctx)
    _LOG.debug("cmd=%s", docker_cmd_)
    rc: Optional[int] = _run(ctx, docker_cmd_, pty=True, **ctx_run_kwargs)
    return rc


@task
def docker_bash(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    entrypoint=True,
    as_user=True,
    generate_docker_compose_file=True,
    container_dir_name=".",
):
    """
    Start a bash shell inside the container corresponding to a stage.

    :param entrypoint: whether to use the `entrypoint` or not
    :param as_user: pass the user / group id or not
    :param generate_docker_compose_file: generate the Docker compose file or not
    """
    _report_task(container_dir_name=container_dir_name)
    cmd = "bash"
    docker_cmd_ = _get_docker_compose_cmd(
        base_image,
        stage,
        version,
        cmd,
        generate_docker_compose_file=generate_docker_compose_file,
        entrypoint=entrypoint,
        as_user=as_user,
    )
    _docker_cmd(ctx, docker_cmd_)


@task
def docker_cmd(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    cmd="",
    as_user=True,
    generate_docker_compose_file=True,
    use_bash=False,
    container_dir_name=".",
):
    """
    Execute the command `cmd` inside a container corresponding to a stage.

    :param as_user: pass the user / group id or not
    :param generate_docker_compose_file: generate or reuse the Docker compose file
    :param use_bash: run command through a shell
    """
    _report_task(container_dir_name=container_dir_name)
    hdbg.dassert_ne(cmd, "")
    # TODO(gp): Do we need to overwrite the entrypoint?
    docker_cmd_ = _get_docker_compose_cmd(
        base_image,
        stage,
        version,
        cmd,
        generate_docker_compose_file=generate_docker_compose_file,
        as_user=as_user,
        use_bash=use_bash,
    )
    _docker_cmd(ctx, docker_cmd_)


# ////////////////////////////////////////////////////////////////////////////////
# Jupyter.
# ////////////////////////////////////////////////////////////////////////////////


def _get_docker_jupyter_cmd(
    base_image: str,
    stage: str,
    version: str,
    port: int,
    self_test: bool,
    *,
    print_docker_config: bool = False,
) -> str:
    cmd = ""
    extra_env_vars = [f"PORT={port}"]
    extra_docker_run_opts = ["--service-ports"]
    service_name = "jupyter_server_test" if self_test else "jupyter_server"
    #
    docker_cmd_ = _get_docker_compose_cmd(
        base_image,
        stage,
        version,
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
    stage="dev",
    version="",
    base_image="",
    auto_assign_port=True,
    port=None,
    self_test=False,
    container_dir_name=".",
):
    """
    Run jupyter notebook server.

    :param auto_assign_port: use the UID of the user and the inferred number of the
        repo (e.g., 4 for `~/src/amp4`) to get a unique port
    """
    _report_task(container_dir_name=container_dir_name)
    if port is None:
        if auto_assign_port:
            uid = os.getuid()
            _LOG.debug("uid=%s", uid)
            git_repo_idx = hgit.get_project_dirname(only_index=True)
            git_repo_idx = int(git_repo_idx)
            _LOG.debug("git_repo_idx=%s", git_repo_idx)
            # We assume that there are no more than `max_idx_per_users` clients.
            max_idx_per_user = 10
            hdbg.dassert_lte(git_repo_idx, max_idx_per_user)
            port = (uid * max_idx_per_user) + git_repo_idx
        else:
            port = 9999
    #
    _LOG.info("Assigned port is %s", port)
    print_docker_config = False
    docker_cmd_ = _get_docker_jupyter_cmd(
        base_image,
        stage,
        version,
        port,
        self_test,
        print_docker_config=print_docker_config,
    )
    _docker_cmd(ctx, docker_cmd_)


# #############################################################################
# Docker image workflows.
# #############################################################################

# TODO(gp): @all Move all the release code to lib_tasks_docker_release.py


def _to_abs_path(filename: str) -> str:
    filename = os.path.abspath(filename)
    hdbg.dassert_path_exists(filename)
    return filename


def _prepare_docker_ignore(ctx: Any, docker_ignore: str) -> None:
    """
    Copy the target docker_ignore in the proper position for `docker build`.
    """
    # Currently there is no built-in way to control which .dockerignore to use.
    # https://stackoverflow.com/questions/40904409
    hdbg.dassert_path_exists(docker_ignore)
    cmd = f"cp -f {docker_ignore} .dockerignore"
    _run(ctx, cmd)


# =============================================================================
# DEV image flow
# =============================================================================
# - A "local" image (which is a release candidate for the DEV image) is built with:
#   ```
#   > docker_build_local_image
#   ```
#   This creates the local image `dev_tools:local.saggese-1.0.0`
# - A qualification process (e.g., running all unit tests and the QA tests) is
#   performed on the "local" image (e.g., locally or through GitHub actions)
# - If the qualification process is passed, the image is released as `dev` on ECR


# Use Docker buildkit or not.
# DOCKER_BUILDKIT = 1
DOCKER_BUILDKIT = 0


# For base_image, we use "" as default instead None since pyinvoke can only infer
# a single type.
@task
def docker_build_local_image(  # type: ignore
    ctx,
    version,
    cache=True,
    base_image="",
    update_poetry=False,
    container_dir_name=".",
    just_do_it=False,
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param update_poetry: run poetry lock to update the packages
    :param just_do_it: execute the action ignoring the checks
    """
    _report_task(container_dir_name=container_dir_name)
    if just_do_it:
        _LOG.warning("Skipping subsequent version check")
    else:
        _dassert_is_subsequent_version(
            version, container_dir_name=container_dir_name
        )
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # Update poetry, if needed.
    if update_poetry:
        cmd = "cd devops/docker_build; poetry lock -v"
        _run(ctx, cmd)
    # Prepare `.dockerignore`.
    docker_ignore = ".dockerignore.dev"
    _prepare_docker_ignore(ctx, docker_ignore)
    # Build the local image.
    image_local = get_image(base_image, "local", version)
    _dassert_is_image_name_valid(image_local)
    # This code path through Git tag was discontinued with CmTask746.
    # git_tag_prefix = get_default_param("BASE_IMAGE")
    # container_version = get_git_tag(version)
    #
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no-cache" if not cache else ""
    # TODO(gp): Use _to_multi_line_cmd()
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        --build-arg AM_CONTAINER_VERSION={version} \
        --tag {image_local} \
        --file {dockerfile} \
        .
    """
    _run(ctx, cmd)
    # Check image and report stats.
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


@task
def docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Mark the "local" image as "dev".

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task(container_dir_name=container_dir_name)
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # Tag local image as versioned dev image (e.g., `dev-1.0.0`).
    image_versioned_local = get_image(base_image, "local", version)
    image_versioned_dev = get_image(base_image, "dev", version)
    cmd = f"docker tag {image_versioned_local} {image_versioned_dev}"
    _run(ctx, cmd)
    # Tag local image as dev image.
    latest_version = None
    image_dev = get_image(base_image, "dev", latest_version)
    cmd = f"docker tag {image_versioned_local} {image_dev}"
    _run(ctx, cmd)


@task
def docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "dev" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task(container_dir_name=container_dir_name)
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    #
    docker_login(ctx)
    # Push Docker versioned tag.
    image_versioned_dev = get_image(base_image, "dev", version)
    cmd = f"docker push {image_versioned_dev}"
    _run(ctx, cmd, pty=True)
    # Push Docker tag.
    latest_version = None
    image_dev = get_image(base_image, "dev", latest_version)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd, pty=True)


@task
def docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    skip_tests=False,
    fast_tests=True,
    slow_tests=True,
    superslow_tests=False,
    # TODO(Nikola): CMTask #2137 - QA test run issues.
    qa_tests=False,
    push_to_repo=True,
    # TODO(Nikola): poetry does not work via GH actions,
    #  see "Switch all the build systems to Python 3.9" CmTask #535.
    update_poetry=False,
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest "dev" image.

    This can be used to test the entire flow from scratch by building an image,
    running the tests, but not necessarily pushing.

    Phases:
    1) Build local image
    2) Run the unit tests (e.g., fast, slow, superslow) on the local image
    3) Mark local as dev image
    4) Run the QA tests on the dev image
    5) Push dev image to the repo

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param skip_tests: skip all the tests and release the dev image
    :param fast_tests: run fast tests, unless all tests skipped
    :param slow_tests: run slow tests, unless all tests skipped
    :param superslow_tests: run superslow tests, unless all tests skipped
    :param qa_tests: run end-to-end linter tests, unless all tests skipped
    :param push_to_repo: push the image to the repo_short_name
    :param update_poetry: update package dependencies using poetry
    """
    _report_task(container_dir_name=container_dir_name)
    # 1) Build "local" image.
    docker_build_local_image(
        ctx,
        cache=cache,
        update_poetry=update_poetry,
        version=version,
        container_dir_name=container_dir_name,
    )
    # Run resolve after `docker_build_local_image` so that a proper check
    # for subsequent version can be made in case `FROM_CHANGELOG` token
    # is used.
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # 2) Run tests for the "local" image.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        fast_tests = False
        slow_tests = False
        superslow_tests = False
        qa_tests = False
    stage = "local"
    if fast_tests:
        run_fast_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    if slow_tests:
        run_slow_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    if superslow_tests:
        run_superslow_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    # 3) Promote the "local" image to "dev".
    docker_tag_local_image_as_dev(
        ctx, version, container_dir_name=container_dir_name
    )
    # 4) Run QA tests for the (local version) of the dev image.
    if qa_tests:
        run_qa_tests(  # type: ignore # noqa: F405
            ctx, stage="dev", version=version
        )
    # 5) Push the "dev" image to ECR.
    if push_to_repo:
        docker_push_dev_image(ctx, version, container_dir_name=container_dir_name)
    else:
        _LOG.warning(
            "Skipping pushing dev image to repo_short_name, as requested"
        )
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# PROD image flow:
# #############################################################################
# - PROD image has no release candidate
# - Start from a DEV image already built and qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image is tagged as "prod"


# TODO(gp): Remove redundancy with docker_build_local_image(), if possible.
@task
def docker_build_prod_image(  # type: ignore
    ctx,
    version,
    cache=True,
    base_image="",
    candidate=False,
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build a prod image.

    Phases:
    - Build the prod image on top of the dev image

    :param version: version to tag the image and code with
    :param cache: note that often the prod image is just a copy of the dev
        image so caching makes no difference
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param candidate: build a prod image with a tag format: prod-{hash}
        where hash is the output of hgit.get_head_hash
    """
    _report_task(container_dir_name=container_dir_name)
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # Prepare `.dockerignore`.
    docker_ignore = ".dockerignore.prod"
    _prepare_docker_ignore(ctx, docker_ignore)
    # TODO(gp): We should do a `i git_clean` to remove artifacts and check that
    #  the client is clean so that we don't release from a dirty client.
    # Build prod image.
    if candidate:
        # For candidate prod images which need to be tested on
        # the AWS infra add a hash identifier.
        latest_version = None
        image_versioned_prod = get_image(base_image, "prod", latest_version)
        head_hash = hgit.get_head_hash(short_hash=True)
        image_versioned_prod += f"-{head_hash}"
    else:
        image_versioned_prod = get_image(base_image, "prod", version)
    _dassert_is_image_name_valid(image_versioned_prod)
    #
    dockerfile = "devops/docker_build/prod.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    # TODO(gp): Use _to_multi_line_cmd()
    opts = "--no-cache" if not cache else ""
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        --tag {image_versioned_prod} \
        --file {dockerfile} \
        --build-arg VERSION={version} \
        .
    """
    _run(ctx, cmd)
    if candidate:
        _LOG.info("Head hash: %s", head_hash)
        cmd = f"docker image ls {image_versioned_prod}"
    else:
        # Tag versioned image as latest prod image.
        latest_version = None
        image_prod = get_image(base_image, "prod", latest_version)
        cmd = f"docker tag {image_versioned_prod} {image_prod}"
        _run(ctx, cmd)
        #
        cmd = f"docker image ls {image_prod}"

    _run(ctx, cmd)


@task
def docker_push_prod_image(  # type: ignore
    ctx,
    version,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "prod" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task(container_dir_name=container_dir_name)
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    #
    docker_login(ctx)
    # Push versioned tag.
    image_versioned_prod = get_image(base_image, "prod", version)
    cmd = f"docker push {image_versioned_prod}"
    _run(ctx, cmd, pty=True)
    #
    latest_version = None
    image_prod = get_image(base_image, "prod", latest_version)
    cmd = f"docker push {image_prod}"
    _run(ctx, cmd, pty=True)


@task
def docker_push_prod_candidate_image(  # type: ignore
    ctx,
    candidate,
    base_image="",
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Push the "prod" candidate image to ECR.

    :param candidate: hash tag of the candidate prod image to push
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task(container_dir_name=container_dir_name)
    #
    docker_login(ctx)
    # Push image with tagged with a hash ID.
    image_versioned_prod = get_image(base_image, "prod", None)
    cmd = f"docker push {image_versioned_prod}-{candidate}"
    _run(ctx, cmd, pty=True)


@task
def docker_release_prod_image(  # type: ignore
    ctx,
    version,
    cache=True,
    skip_tests=False,
    fast_tests=True,
    slow_tests=True,
    superslow_tests=False,
    push_to_repo=True,
    container_dir_name=".",
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the prod image.

    - Build prod image
    - Run the tests
    - Push the prod image repo

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param skip_tests: skip all the tests and release the dev image
    :param fast_tests: run fast tests, unless all tests skipped
    :param slow_tests: run slow tests, unless all tests skipped
    :param superslow_tests: run superslow tests, unless all tests skipped
    :param push_to_repo: push the image to the repo_short_name
    """
    _report_task(container_dir_name=container_dir_name)
    version = _resolve_version_value(
        version, container_dir_name=container_dir_name
    )
    # 1) Build prod image.
    docker_build_prod_image(
        ctx, cache=cache, version=version, container_dir_name=container_dir_name
    )
    # 2) Run tests.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        fast_tests = slow_tests = superslow_tests = False
    stage = "prod"
    if fast_tests:
        run_fast_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    if slow_tests:
        run_slow_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    if superslow_tests:
        run_superslow_tests(  # type: ignore # noqa: F405
            ctx, stage=stage, version=version
        )
    # 3) Push prod image.
    if push_to_repo:
        docker_push_prod_image(
            ctx, version=version, container_dir_name=container_dir_name
        )
    else:
        _LOG.warning("Skipping pushing image to repo_short_name as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_release_all(ctx, version, container_dir_name="."):  # type: ignore
    """
    (ONLY CI/CD) Release both dev and prod image to ECR.

    This includes:
    - docker_release_dev_image
    - docker_release_prod_image

    :param version: version to tag the image and code with
    """
    _report_task()
    docker_release_dev_image(ctx, version, container_dir_name=container_dir_name)
    docker_release_prod_image(ctx, version, container_dir_name=container_dir_name)
    _LOG.info("==> SUCCESS <==")


def _docker_rollback_image(
    ctx: Any, base_image: str, stage: str, version: str
) -> None:
    """
    Rollback the versioned image for a particular stage.

    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param stage: select a specific stage for the Docker image
    :param version: version to tag the image and code with
    """
    image_versioned_dev = get_image(base_image, stage, version)
    latest_version = None
    image_dev = get_image(base_image, stage, latest_version)
    cmd = f"docker tag {image_versioned_dev} {image_dev}"
    _run(ctx, cmd)


@task
def docker_rollback_dev_image(  # type: ignore
    ctx,
    version,
    push_to_repo=True,
):
    """
    Rollback the version of the dev image.

    Phases:
    1) Ensure that version of the image exists locally
    2) Promote versioned image as dev image
    3) Push dev image to the repo

    :param version: version to tag the image and code with
    :param push_to_repo: push the image to the ECR repo
    """
    _report_task()
    # 1) Ensure that version of the image exists locally.
    _docker_pull(ctx, base_image="", stage="dev", version=version)
    # 2) Promote requested image as dev image.
    _docker_rollback_image(ctx, base_image="", stage="dev", version=version)
    # 3) Push the "dev" image to ECR.
    if push_to_repo:
        docker_push_dev_image(ctx, version=version)
    else:
        _LOG.warning("Skipping pushing dev image to ECR, as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_rollback_prod_image(  # type: ignore
    ctx,
    version,
    push_to_repo=True,
):
    """
    Rollback the version of the prod image.

    Same as parameters and meaning as `docker_rollback_dev_image`.
    """
    _report_task()
    # 1) Ensure that version of the image exists locally.
    _docker_pull(ctx, base_image="", stage="prod", version=version)
    # 2) Promote requested image as prod image.
    _docker_rollback_image(ctx, base_image="", stage="prod", version=version)
    # 3) Push the "prod" image to ECR.
    if push_to_repo:
        docker_push_prod_image(ctx, version=version)
    else:
        _LOG.warning("Skipping pushing prod image to ECR, as requested")
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# Fix permission
# #############################################################################


# The desired invariants are that all files
# 1) are owned by our user or by Docker user
# 2) have the shared group as group
# 3) have the same user and group permissions

# E.g.,
# -rw-rw-r-- 1 spm-sasm spm-sasm-fileshare 21877 Nov  3 18:11 pytest_logger.log

# The possible problems are:
# -r--r--r-- 1 spm-sasm spm-sasm-fileshare ./.git/objects/02/4df16f66c87bdfb
# -rw-r--r-- 1 265533 spm-sasm-fileshare  ./core_lime/dataflow/nodes/test/te
# -rw-rw-r-- 1 265533 spm-sasm-fileshare  ./research/real_time/notebooks/Lim

# drwxr-sr-x 2 gsaggese spm-sasm-fileshare    35 Oct 12 21:51 test
# chmod g=u amp/dev_scripts/git/git_hooks/test


def _save_dir_status(dir_name: str, filename: str) -> None:
    cmd = f'find {dir_name} -name "*" | sort | xargs ls -ld >{filename}'
    hsystem.system(cmd)
    _LOG.info("Saved dir status in %s", filename)


# From https://stackoverflow.com/questions/1830618
def _get_user_group(filename: str) -> Tuple[str, str]:
    """
    Return the symbolic name of user and group of a file.
    """
    uid = os.stat(filename).st_uid
    try:
        user = pwd.getpwuid(uid).pw_name
    except KeyError as e:
        # _LOG.warning("Error: ", str(e))
        _ = e
        user = str(uid)
    #
    gid = os.stat(filename).st_gid
    try:
        group = grp.getgrgid(gid).gr_name
    except KeyError as e:
        _ = e
        group = str(gid)
    return user, group


def _find_files_for_user(dir_name: str, user: str, is_equal: bool) -> List[str]:
    """
    Find all the files under `abs_dir` that are owned or not by `user`.
    """
    _LOG.debug("")
    mode = "\\!" if not is_equal else ""
    cmd = f'find {dir_name} -name "*" {mode} -user "{user}"'
    _, txt = hsystem.system_to_string(cmd)
    files: List[str] = txt.split("\n")
    return files


def _find_files_for_group(dir_name: str, group: str, is_equal: bool) -> List[str]:
    """
    Find all the files under `abs_dir` that are owned by a group `group`.
    """
    _LOG.debug("")
    mode = "\\!" if not is_equal else ""
    cmd = f'find {dir_name} -name "*" {mode} -group "{group}"'
    _, txt = hsystem.system_to_string(cmd)
    files: List[str] = txt.split("\n")
    return files


def _compute_stats_by_user_and_group(dir_name: str) -> Tuple[Dict, Dict, Dict]:
    """
    Scan all the files reporting statistics in terms of users and groups.

    It also compute a mapping from file to user and group.
    """
    _LOG.debug("")
    # Find all files.
    cmd = f'find {dir_name} -name "*"'
    _, txt = hsystem.system_to_string(cmd)
    files = txt.split("\n")
    # Get the user of each file.
    user_to_files: Dict[str, List[str]] = {}
    group_to_files: Dict[str, List[str]] = {}
    file_to_user_group: Dict[str, Tuple[str, str]] = {}
    for file in files:
        user, group = _get_user_group(file)
        # Update mapping from user to files.
        if user not in user_to_files:
            user_to_files[user] = []
        user_to_files[user].append(file)
        # Update mapping from group to files.
        if group not in group_to_files:
            group_to_files[group] = []
        group_to_files[group].append(file)
        # Update the mapping from file to (user, group).
        hdbg.dassert_not_in(file, file_to_user_group)
        file_to_user_group[file] = (user, group)
    # Print stats.
    txt1 = ""
    for user, files in user_to_files.items():
        txt1 += f"{user}({len(files)}), "
    _LOG.info("user=%s", txt1)
    #
    txt2 = ""
    for group, files in group_to_files.items():
        txt2 += f"{group}({len(files)}), "
    _LOG.info("group=%s", txt2)
    return user_to_files, group_to_files, file_to_user_group


def _ls_l(files: List[str], size: int = 100) -> str:
    """
    Run `ls -l` on the files using chunks of size `size`.
    """
    txt = []
    for pos in range(0, len(files), size):
        files_tmp = files[pos : pos + size]
        files_tmp = [f"'{f}'" for f in files_tmp]
        cmd = f"ls -ld {' '.join(files_tmp)}"
        _, txt_tmp = hsystem.system_to_string(cmd)
        txt.append(txt_tmp)
    return "\n".join(txt)


def _exec_cmd_by_chunks(
    cmd: str, files: List[str], abort_on_error: bool, size: int = 100
) -> None:
    """
    Execute `cmd` on files using chunks of size `size`.
    """
    for pos in range(0, len(files), size):
        files_tmp = files[pos : pos + size]
        files_tmp = [f"'{f}'" for f in files_tmp]
        cmd = f"{cmd} {' '.join(files_tmp)}"
        hsystem.system(cmd, abort_on_error=abort_on_error)


def _print_problems(dir_name: str = ".") -> None:
    """
    Do `ls -l` on files that are not owned by the current user and its group.

    This function is used for debugging.
    """
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    user = hsystem.get_user_name()
    docker_user = hgit.execute_repo_config_code("get_docker_user()")
    # user_group = f"{user}_g"
    # shared_group = hgit.execute_repo_config_code("get_docker_shared_group()")
    files_with_problems = []
    for file, (curr_user, curr_group) in file_to_user_group.items():
        _ = curr_user, curr_group
        # Files owned by our user and
        # if curr_user == user and curr_group == user_group:
        #    continue
        if curr_user in (user, docker_user):
            continue
        # if curr_group == shared_group:
        #    continue
        files_with_problems.append(file)
    #
    txt = _ls_l(files_with_problems)
    print(txt)


def _change_file_ownership(file: str, abort_on_error: bool) -> None:
    """
    Change ownership of files with an invalid user (e.g., 265533) by copying
    and deleting.
    """
    # pylint: disable=line-too-long
    # > ls -l ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py
    # -rw-r--r-- 1 265533 spm-sasm-fileshare 14327 Nov  3 14:01 ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py
    #
    # > mv ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py{,.OLD}
    #
    # > cp ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py{.OLD,}
    #
    # > ls -l ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py
    # -rw-r--r-- 1 gsaggese spm-sasm-fileshare 14327 Nov  5 17:58 ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py
    #
    # > rm -rf ./core_lime/dataflow/nodes/test/test_core_lime_dataflow_nodes.py.OLD
    # pylint: enable=line-too-long
    hdbg.dassert_file_exists(file)
    tmp_file = file + ".OLD"
    #
    cmd = f"mv {file} {tmp_file}"
    hsystem.system(cmd, abort_on_error=abort_on_error)
    #
    cmd = f"cp {tmp_file} {file}"
    hsystem.system(cmd, abort_on_error=abort_on_error)
    #
    cmd = f"rm -rf {tmp_file}"
    hsystem.system(cmd, abort_on_error=abort_on_error)


def _fix_invalid_owner(dir_name: str, fix: bool, abort_on_error: bool) -> None:
    """
    Fix files that are owned by a user that is not the current user or the
    Docker one.
    """
    _LOG.info("\n%s", hprint.frame(hintros.get_function_name()))
    #
    _LOG.info("Before fix")
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    #
    user = hsystem.get_user_name()
    docker_user = hgit.execute_repo_config_code("get_docker_user()")
    for file, (curr_user, _) in tqdm.tqdm(file_to_user_group.items()):
        if curr_user not in (user, docker_user):
            _LOG.info("Fixing file '%s'", file)
            hdbg.dassert_file_exists(file)
            cmd = f"ls -l {file}"
            hsystem.system(
                cmd, abort_on_error=abort_on_error, suppress_output=False
            )
            if fix:
                _change_file_ownership(file, abort_on_error)
    #
    _LOG.info("After fix")
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)


def _fix_group(dir_name: str, fix: bool, abort_on_error: bool) -> None:
    """
    Ensure that all files are owned by the shared group.
    """
    _LOG.info("\n%s", hprint.frame(hintros.get_function_name()))
    _LOG.info("Before fix")
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    if fix:
        # Get the user and the group.
        user = hsystem.get_user_name()
        user_group = f"{user}_g"
        shared_group = hgit.execute_repo_config_code("get_docker_shared_group()")
        #
        for file, (curr_user, curr_group) in file_to_user_group.items():
            # If the group is the shared group there is nothing to do.
            if curr_group == shared_group:
                continue
            cmd = f"chgrp {shared_group} {file}"
            if curr_user == user:
                # This is a paranoia check.
                hdbg.dassert_eq(curr_group, user_group)
            else:
                # For files not owned by the current user, we need to `sudo`.
                cmd = f"sudo -u {curr_user} {cmd}"
            hsystem.system(cmd, abort_on_error=abort_on_error)
        _LOG.info("After fix")
        _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    else:
        _LOG.warning("Skipping fix")


def _fix_group_permissions(dir_name: str, abort_on_error: bool) -> None:
    """
    Ensure that all files are owned by the shared group.
    """
    _LOG.info("\n%s", hprint.frame(hintros.get_function_name()))
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    user = hsystem.get_user_name()
    # docker_user = get_default_param("DOCKER_USER")
    for file, (curr_user, curr_group) in tqdm.tqdm(file_to_user_group.items()):
        _ = curr_group
        st_mode = os.stat(file).st_mode
        perms = oct(st_mode & 0o777)
        # perms=0o775
        if perms[2] != perms[3]:
            _LOG.debug("%s -> %s, %s", file, oct(st_mode), perms)
            cmd = f"chmod g=u {file}"
            if curr_user != user:
                # For files not owned by the current user, we need to `sudo`.
                cmd = f"sudo -u {curr_user} {cmd}"
            hsystem.system(cmd, abort_on_error=abort_on_error)
        is_dir = os.path.isdir(file)
        if is_dir:
            # pylint: disable=line-too-long
            # From https://www.gnu.org/software/coreutils/manual/html_node/Directory-Setuid-and-Setgid.html
            # If a directory
            # inherit the same group as the directory,
            # pylint: enable=line-too-long
            has_set_group_id = st_mode & stat.S_ISGID
            if not has_set_group_id:
                cmd = f"chmod g+s {file}"
                if curr_user != user:
                    # For files not owned by the current user, we need to `sudo`.
                    cmd = f"sudo -u {curr_user} {cmd}"
                hsystem.system(cmd, abort_on_error=abort_on_error)


@task
def fix_perms(  # type: ignore
    ctx, dir_name=".", action="all", fix=True, abort_on_error=True
):
    """
    :param action:
        - `all`: run all the fixes
        - `print_stats`: print stats about file users and groups
        - `print_problems`:
        - `fix_invalid_owner`: fix the files with an invalid owner (e.g., mysterious
            265533)
        - `fix_group`: ensure that shared group owns all the files
        - `fix_group_permissions`: ensure that the group permissions are the same
            as the owner ones
    """
    _ = ctx
    _report_task()
    #
    if hgit.execute_repo_config_code("is_dev4()"):
        if action == "all":
            action = ["fix_invalid_owner", "fix_group", "fix_group_permissions"]
        else:
            action = [action]
        #
        file_name1 = "./tmp.fix_perms.before.txt"
        _save_dir_status(dir_name, file_name1)
        #
        if "print_stats" in action:
            _compute_stats_by_user_and_group(dir_name)
        if "print_problems" in action:
            _print_problems(dir_name)
        if "fix_invalid_owner" in action:
            _fix_invalid_owner(dir_name, fix, abort_on_error)
        if "fix_group" in action:
            _fix_group(dir_name, fix, abort_on_error)
        if "fix_group_permissions" in action:
            _fix_group_permissions(dir_name, abort_on_error)
        #
        file_name2 = "./tmp.fix_perms.after.txt"
        _save_dir_status(dir_name, file_name2)
        #
        cmd = f"To compare run:\n> vimdiff {file_name1} {file_name2}"
        print(cmd)
    elif hgit.execute_repo_config_code("is_dev_ck()"):
        user = hsystem.get_user_name()
        group = user
        cmd = f"sudo chown -R {user}:{group} *"
        hsystem.system(cmd)
        cmd = f"sudo chown -R {user}:{group} .pytest_cache"
        hsystem.system(cmd, abort_on_error=False)
    else:
        raise ValueError(f"Invalid machine {os.uname()[1]}")


def _update_task_definition(task_definition: str, image_tag: str) -> None:
    """
    Create the new revision of specified ECS task definition and point 
    Image URL specified to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update 
    to container image URL is made, e.g. `cmamp-test`
    :param image_tag: the hash of the new candidate image, e.g. `13538588e`
    """
    client = hsecret.get_ecs_client("ck")
    # Get the last revison of the task definition.
    task_description = client.describe_task_definition(
    taskDefinition=task_definition
    )
    task_def = task_description["taskDefinition"]
    old_image = task_def["containerDefinitions"][0]["image"]
    # Edit container version, e.g. `cmamp:prod-12a45` - > cmamp:prod-12b46`
    new_image = re.sub("prod-(.+)$", f"prod-{image_tag}", old_image)
    task_def["containerDefinitions"][0]["image"] = new_image
    # Register the new revision with the new image.
    client.register_task_definition(
        family=task_definition,
        taskRoleArn=task_def["taskRoleArn"],
        executionRoleArn=task_def["taskRoleArn"],
        networkMode=task_def["networkMode"],
        containerDefinitions=task_def["containerDefinitions"],
        volumes=task_def["volumes"],
        placementConstraints=task_def["placementConstraints"],
        requiresCompatibilities=task_def["requiresCompatibilities"],
        cpu=task_def["cpu"],
        memory=task_def["memory"]
    )
    return

@task
def docker_create_candidate_image(ctx, task_definition: str):  # type: ignore
    """
    Create new prod candidate image and update the specified ECS task definition such that
    the Image URL specified in container definition points to the new candidate image.

    :param task_definition: the name of the ECS task definition for which an update 
    to container image URL is made, e.g. `cmamp-test`
    """
    # Get latest version.
    last_version = hversio.get_changelog_version(".")
    # Create new prod image.
    #cmd = f"invoke docker_build_prod_image -v {last_version} --candidate"
    out = docker_build_prod_image(
        ctx,
        last_version,
        candidate=True
    )
    # Get the hash of the image.
    tag = hgit.get_head_hash(".", short_hash=True)
    # Push candidate image.
    out = docker_push_prod_candidate_image(ctx, tag)
    # Register new task definition revision with updated image URL. 
    #_update_task_definition(task_definition, tag)
    return

# TODO(gp): Add gh_open_pr to jump to the PR from this branch.

# TODO(gp): Add ./dev_scripts/testing/pytest_count_files.sh

# pylint: disable=line-too-long
# From https://stackoverflow.com/questions/34878808/finding-docker-container-processes-from-host-point-of-view
# Convert Docker container to processes id
# for i in $(docker container ls --format "{{.ID}}"); do docker inspect -f '{{.State.Pid}} {{.Name}}' $i; done
# 7444 /compose_app_run_d386dc360071
# 8857 /compose_jupyter_server_run_7575f1652032
# 1767 /compose_app_run_6782c2bd6999
# 25163 /compose_app_run_ab27e17f2c47
# 18721 /compose_app_run_de23819a6bc2
# pylint: enable=line-too-long
