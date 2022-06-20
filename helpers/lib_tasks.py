"""
Import as:

import helpers.lib_tasks as hlibtask
"""

import grp
import logging
import os
import pwd
import re
import stat
from typing import Dict, List, Tuple

import tqdm
from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hintrospection as hintros
import helpers.hprint as hprint
import helpers.hsystem as hsystem

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
from helpers.lib_tasks_utils import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import

_LOG = logging.getLogger(__name__)


# Conventions around `pyinvoke`:
# - `pyinvoke` uses introspection to infer properties of a task, but doesn't
#   support many Python3 features (see https://github.com/pyinvoke/invoke/issues/357)
# - Don't use type hints in `@tasks`
#   - we use `# ignore: type` to avoid mypy complaints
# - Minimize the code in `@tasks` calling other functions to use Python3 features
# - Use `""` as default instead None since `pyinvoke` can only infer a single type


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
