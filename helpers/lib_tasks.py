"""
Import as:

import helpers.lib_tasks as hlibtask
"""

import datetime
import glob
import grp
import logging
import os
import pprint
import pwd
import re
import stat
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import tqdm
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

# Import this way to avoid complexity in propagating the refactoring in all
# the repos downstream.
from helpers.lib_tasks_docker import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_docker_release import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
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
_DEFAULT_PARAMS = {}


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
