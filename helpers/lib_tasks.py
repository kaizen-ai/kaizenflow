"""
Import as:

import helpers.lib_tasks as ltasks
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
from typing import Any, Dict, List, Match, Optional, Union

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
import helpers.versioning as hversi

_LOG = logging.getLogger(__name__)

# #############################################################################
# Default params.
# #############################################################################

# By default we run against the dev image.
STAGE = "dev"

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

# pyinvoke `ctx.run()` is useful for unit testing, since it allows to:
# - mock the result of a system call
# - register the issued command line (to create the expected outcome of a test)
# On the other side `system_interaction.py` contains many utilities that make
# it easy to interact with the system.
# Once AmpPart1347 is implemented we can replace all the `ctx.run()` with calls
# to `system_interaction.py`.


_IS_FIRST_CALL = False


def _report_task(txt: str = "") -> None:
    # On the first invocation report the version.
    global _IS_FIRST_CALL
    if _IS_FIRST_CALL:
        _IS_FIRST_CALL = True
        hversi.check_version()
    # Print the name of the function.
    func_name = hintros.get_function_name(count=1)
    msg = "## %s: %s" % (func_name, txt)
    print(hprint.color_highlight(msg, color="purple"))


# TODO(gp): Move this to helpers.system_interaction and allow to add the switch
#  globally.
def _to_single_line_cmd(cmd: Union[str, List[str]]) -> str:
    """
    Convert a multiline command (as a string or list of strings) into a single
    line.

    E.g., convert
        ```
        IMAGE=665840871993.../amp:dev \
            docker-compose \
            --file devops/compose/docker-compose.yml \
            --file devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env
        ```
    into
        ```
        IMAGE=665840871993.../amp:dev docker-compose --file ...
        ```
    """
    if isinstance(cmd, list):
        cmd = " ".join(cmd)
    dbg.dassert_isinstance(cmd, str)
    cmd = cmd.rstrip().lstrip()
    # Remove `\` at the end of the line.
    cmd = re.sub(r" \\\s*$", " ", cmd, flags=re.MULTILINE)
    # Use a single space between words in the command.
    # TODO(gp): This is a bit dangerous if there are multiple spaces in a string
    #  that for some reason are meaningful.
    cmd = " ".join(cmd.split())
    return cmd


def _to_multi_line_cmd(docker_cmd_: List[str]) -> str:
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
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:dev \
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


# TODO(gp): Pass through command line using a global switch or an env var.
use_one_line_cmd = False


def _run(ctx: Any, cmd: str, *args: Any, **kwargs: Any) -> None:
    _LOG.debug("cmd=%s", cmd)
    if use_one_line_cmd:
        cmd = _to_single_line_cmd(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, *args, **kwargs)


def _get_files_to_process(
    modified: bool,
    branch: bool,
    last_commit: bool,
    files_from_user: str,
    mutually_exclusive: bool,
    remove_dirs: bool,
) -> List[str]:
    """
    Get a list of files to process.

    The files are the ones that are:
    - changed in the branch
    - changed in the client (both staged and modified)
    - part of the previous commit
    - passed by the user

    :param modified: return files modified in the client (i.e., changed with
        respect to HEAD)
    :param branch: return files modified with respect to the branch point
    :param last_commit: return files part of the previous commit
    :param files_from_user: return files passed to this function
    :param mutually_exclusive: ensure that all options are mutually exclusive
    """
    _LOG.debug(
        hprint.to_str(
            "modified branch last_commit files_from_user "
            "mutually_exclusive remove_dirs"
        )
    )
    if mutually_exclusive:
        dbg.dassert_eq(
            int(modified)
            + int(branch)
            + int(last_commit)
            + int(len(files_from_user) > 0),
            1,
            msg="You need to specify exactly one option among --modified, --branch, "
            "--last_commit, and --files",
        )
    else:
        dbg.dassert_eq(
            int(modified) + int(branch) + int(last_commit),
            1,
            msg="You need to specify exactly one among --modified, --branch, "
            "--last_commit",
        )
    if modified:
        files = git.get_modified_files(".")
    elif branch:
        files = git.get_modified_files_in_branch("master", ".")
    elif last_commit:
        files = git.get_previous_committed_files(".")
    if files_from_user:
        # If files were passed, overwrite the previous decision.
        files = files_from_user.split(" ")
    # Convert into a list.
    dbg.dassert_isinstance(files, list)
    files_to_process = [f for f in files if f != ""]
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Remove dirs, if needed.
    if remove_dirs:
        hsinte.remove_dirs(files_to_process)
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Ensure that there are files to process.
    if not files_to_process:
        _LOG.warning("No files were selected")
    return files_to_process


# Copied from helpers.datetime_ to avoid dependency from pandas.


def _get_timestamp(utc: bool = False) -> str:
    if utc:
        timestamp = datetime.datetime.utcnow()
    else:
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
    var_names = "ECR_BASE_PATH BASE_IMAGE".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_param(v)))


@task
def print_tasks(ctx, as_code=False):  # type: ignore
    """
    Print all the available tasks in `lib_tasks.py`.

    These tasks might be exposed or not by different.

    :param as_python_code: print as python code so that it can be embed in a
        `from helpers.lib_tasks import ...`
    """
    _report_task()
    _ = ctx
    func_names = []
    # TODO(gp): Use __file__ instead of hardwiring the file.
    cmd = r'\grep "^@task" -A 1 helpers/lib_tasks.py | grep def'
    # def print_setup(ctx):  # type: ignore
    # def git_pull(ctx):  # type: ignore
    # def git_pull_master(ctx):  # type: ignore
    _, txt = hsinte.system_to_string(cmd)
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
def git_create_branch(  # type: ignore
    ctx,
    branch_name="",
    issue_id=0,
    repo="current",
    suffix="",
    only_branch_from_master=True,
):
    """
    Create and push upstream branch `branch_name` or the one corresponding to
    `issue_id` in repo `repo`.

    E.g.,
    ```
    > git checkout -b LemTask169_Get_GH_actions
    > git push --set- upstream origin LemTask169_Get_GH_actions
    ```

    :param branch_name: name of the branch to create (e.g.,
        `LemTask169_Get_GH_actions`)
    :param issue_id: use the canonical name for the branch corresponding to that
        issue
    :param repo: name of the GitHub repo that the `issue_id` belongs to
        - "current" (default): the current repo
        - short name (e.g., "amp", "lem") of the branch
    :param suffix: suffix (e.g., "02") to add to the branch name when using issue_id
    :param only_branch_from_master: only allow to branch from master
    """
    _report_task()
    if issue_id > 0:
        # User specified an issue id on GitHub.
        dbg.dassert_eq(
            branch_name, "", "You can't specify both --issue and --branch_name"
        )
        branch_name = _get_gh_issue_title(issue_id, repo)
        _LOG.info(
            "Issue %d in %s repo corresponds to '%s'", issue_id, repo, branch_name
        )
        if suffix != "":
            # Add the the suffix.
            _LOG.debug("Adding suffix '%s' to '%s'", suffix, branch_name)
            if suffix[0] in ("-", "_"):
                _LOG.warning(
                    "Suffix '%s' should not start with '%s': removing",
                    suffix,
                    suffix[0],
                )
                suffix = suffix.rstrip("-_")
            branch_name += "_" + suffix
    #
    _LOG.info("branch_name='%s'", branch_name)
    dbg.dassert_ne(branch_name, "")
    # Make sure we are branching from `master`, unless that's what the user wants.
    curr_branch = git.get_branch_name()
    if curr_branch != "master":
        if only_branch_from_master:
            dbg.dfatal(
                "You should branch from master and not from '%s'" % curr_branch
            )
    # Fetch master.
    cmd = "git pull --autostash"
    _run(ctx, cmd)
    # git checkout -b LemTask169_Get_GH_actions_working_on_lemonade
    cmd = f"git checkout -b {branch_name}"
    _run(ctx, cmd)
    # TODO(gp): If the branch already exists, increase the number.
    #   git checkout -b AmpTask1329_Review_code_in_core_03
    #   fatal: A branch named 'AmpTask1329_Review_code_in_core_03' already exists.
    #   saggese@gpmaclocal.local ==> RC: 128 <==
    cmd = f"git push --set-upstream origin {branch_name}"
    _run(ctx, cmd)


@task
def git_create_patch(  # type: ignore
    ctx, mode="diff", modified=False, branch=False, last_commit=False, files=""
):
    """
    Create a patch file for the entire repo client from the base revision. This
    script accepts a list of files to package, if specified.

    :param mode: what kind of patch to create
        - "diff": (default) creates a patch with the diff of the files
        - "tar": creates a tar ball with all the files
    :param modified: select the files modified in the client
    :param branch: select the files modified in the current branch
    :param last_commit: select the files modified in the previous commit
    :param files: specify a space-separated list of files
    """
    _report_task(hprint.to_str("mode modified branch last_commit files"))
    _ = ctx
    dbg.dassert_in(mode, ("tar", "diff"))
    # For now we just create a patch for the current submodule.
    super_module = False
    git_client_root = git.get_client_root(super_module)
    hash_ = git.get_head_hash(git_client_root, short_hash=True)
    timestamp = _get_timestamp(utc=False)
    #
    tag = os.path.basename(git_client_root)
    dst_file = f"patch.{tag}.{hash_}.{timestamp}"
    if mode == "tar":
        dst_file += ".tgz"
    elif mode == "diff":
        dst_file += ".patch"
    else:
        dbg.dfatal("Invalid code path")
    _LOG.debug("dst_file=%s", dst_file)
    # Summary of files.
    _LOG.info(
        "Difference between HEAD and master:\n%s",
        git.get_summary_files_in_branch("master", "."),
    )
    # Get the files.
    # We allow to specify files as a subset of files modified in the branch or
    # in the client.
    mutually_exclusive = False
    # We don't allow to specify directories.
    remove_dirs = True
    files_as_list = _get_files_to_process(
        modified, branch, last_commit, files, mutually_exclusive, remove_dirs
    )
    _LOG.info("Files to save:\n%s", hprint.indent("\n".join(files_as_list)))
    if not files_as_list:
        _LOG.warning("Nothing to patch: exiting")
        return
    files_as_str = " ".join(files_as_list)

    # Prepare the patch command.
    cmd = ""
    if mode == "tar":
        cmd = f"tar czvf {dst_file} {files_as_str}"
        cmd_inv = "tar xvzf"
    elif mode == "diff":
        if modified:
            opts = "HEAD"
        elif branch:
            opts = "master..."
        elif last_commit:
            opts = "HEAD^"
        else:
            dbg.dfatal(
                "You need to specify one among -modified, --branch, "
                "--last-commit"
            )
        cmd = f"git diff {opts} --binary {files_as_str} >{dst_file}"
        cmd_inv = "git apply"
    # Execute patch command.
    _LOG.info("Creating the patch into %s", dst_file)
    dbg.dassert_ne(cmd, "")
    _LOG.debug("cmd=%s", cmd)
    rc = hsinte.system(cmd, abort_on_error=False)
    if not rc:
        _LOG.warning("Command failed with rc=%d", rc)
    # Print message to apply the patch.
    remote_file = os.path.basename(dst_file)
    abs_path_dst_file = os.path.abspath(dst_file)
    msg = f"""
# To apply the patch and execute:
> git checkout {hash_}
> {cmd_inv} {abs_path_dst_file}

# To apply the patch to a remote client:
> export SERVER="server"
> export CLIENT_PATH="~/src"
> scp {dst_file} $SERVER:
> ssh $SERVER 'cd $CLIENT_PATH && {cmd_inv} ~/{remote_file}'"
    """
    print(msg)


@task
def git_branch_files(ctx):  # type: ignore
    """
    Report which files are changed in the current branch with respect to
    master.
    """
    _report_task()
    _ = ctx
    print(
        "Difference between HEAD and master:\n"
        + git.get_summary_files_in_branch("master", ".")
    )


@task
def git_last_commit_files(ctx, pbcopy=True):  # type: ignore
    """
    Print the status of the files in the previous commit.

    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    cmd = 'git log -1 --name-status --pretty=""'
    _run(ctx, cmd)
    # Get the list of existing files.
    files = git.get_previous_committed_files(".")
    txt = "\n".join(files)
    print(f"\n# The files modified are:\n{txt}")
    # Save to clipboard.
    res = " ".join(files)
    _to_pbcopy(res, pbcopy)


# TODO(gp): Add the following scripts:
# dev_scripts/git/git_backup.sh
# dev_scripts/git/gcl
# dev_scripts/git/git_branch.sh
# dev_scripts/git/git_branch_point.sh
# dev_scripts/create_class_diagram.sh

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
    List all the running containers.

    ```
    > docker_ps
    CONTAINER ID  user  IMAGE                    COMMAND                    CREATED        STATUS        PORTS  service
    2ece37303ec9  gp    083233266530....:latest  "./docker_build/entry.sh"  5 seconds ago  Up 4 seconds         user_space
    ```
    """
    _report_task()
    # pylint: enable=line-too-long
    fmt = (
        r"""table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}"""
        + r"\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}"
        + r'\t{{.Label "com.docker.compose.service"}}'
    )
    cmd = f"docker ps --format='{fmt}'"
    cmd = _to_single_line_cmd(cmd)
    _run(ctx, cmd)


def _get_last_container_id() -> str:
    # Get the last started container.
    cmd = "docker ps -l | grep -v 'CONTAINER ID'"
    # CONTAINER ID   IMAGE          COMMAND                  CREATED
    # 90897241b31a   eeb33fe1880a   "/bin/sh -c '/bin/ba…"   34 hours ago ...
    _, txt = hsinte.system_to_one_line(cmd)
    # Parse the output: there should be at least one line.
    dbg.dassert_lte(1, len(txt.split(" ")), "Invalid output='%s'", txt)
    container_id: str = txt.split(" ")[0]
    return container_id


@task
def docker_stats(  # type: ignore
    ctx, all=False  # pylint: disable=redefined-builtin
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
    _report_task(hprint.to_str("all"))
    _ = ctx
    fmt = (
        r"table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
        + r"\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}"
    )
    cmd = f"docker stats --no-stream --format='{fmt}'"
    _, txt = hsinte.system_to_string(cmd)
    if all:
        output = txt
    else:
        # Get the id of the last started container.
        container_id = _get_last_container_id()
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
        dbg.dassert_lte(
            len(output), 2, "Invalid output='%s' for '%s'", output, txt
        )
        output = "\n".join(output)
    print(output)


@task
def docker_kill(  # type: ignore
    ctx, all=False  # pylint: disable=redefined-builtin
):
    """
    Kill the last Docker container started.

    :param all: kill all the containers (be careful!)
    """
    _report_task(hprint.to_str("all"))
    # TODO(gp): Ask if we are sure and add a --just-do-it option.
    # Last container.
    opts = "-l"
    if all:
        opts = "-a"
    # Print the containers that will be terminated.
    _run(ctx, f"docker ps {opts}")
    # Kill.
    _run(ctx, f"docker rm -f $(docker ps {opts} -q)")


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
def docker_pull(ctx, stage=STAGE, images="all"):  # type: ignore
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
            image = get_image(stage, base_image)
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
    if hsinte.is_inside_ci():
        _LOG.warning("Running inside GitHub Action: skipping `docker_login`")
        return
    major_version = _get_aws_cli_version()
    # docker login \
    #   -u AWS \
    #   -p eyJ... \
    #   -e none \
    #   https://665840871993.dkr.ecr.us-east-1.amazonaws.com
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
    # cmd = ("aws ecr get-login-password" +
    #       " | docker login --username AWS --password-stdin "
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
    path, _ = git.get_path_from_supermodule()
    docker_compose_path: Optional[str]
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


# TODO(gp): Isn't this in helper.git?
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


def get_image(stage: str, base_image: str) -> str:
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
    image = get_image(stage, base_image)
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
    #
    repo_short_name = git.get_repo_short_name(
        git.get_repo_full_name_from_dirname(".")
    )
    _LOG.debug("repo_short_name=%s", repo_short_name)
    if repo_short_name == "amp":
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
        docker_config_cmd_as_str = _to_multi_line_cmd(docker_config_cmd)
        _LOG.debug("docker_config_cmd=\n%s", docker_config_cmd_as_str)
        _LOG.debug(
            "docker_config=\n%s",
            hsinte.system_to_string(docker_config_cmd_as_str)[1],
        )
    # Print the config for debugging purpose.
    docker_cmd_ = _to_multi_line_cmd(docker_cmd_)
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
def docker_bash(ctx, stage=STAGE, entrypoint=True):  # type: ignore
    """
    Start a bash shell inside the container corresponding to a stage.
    """
    _report_task()
    base_image = ""
    cmd = "bash"
    docker_cmd_ = _get_docker_cmd(stage, base_image, cmd, entrypoint=entrypoint)
    _docker_cmd(ctx, docker_cmd_)


@task
def docker_cmd(ctx, stage=STAGE, cmd=""):  # type: ignore
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
    stage=STAGE,
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


def _get_build_tag(code_ver: str) -> str:
    """
    Return a string to tag the build.

    E.g.,
    build_tag=
        amp-1.0.0-20210428-
        AmpTask1280_Use_versioning_to_keep_code_and_container_in_sync-
        500a9e31ee70e51101c1b2eb82945c19992fa86e

    :param code_ver: the value from hversi.get_code_version()
    """
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
#   image (e.g., through GitHub actions)
# - If qualification is passed, it becomes "latest".


# For base_image, we use "" as default instead None since pyinvoke can only infer
# a single type.
@task
def docker_build_local_image(  # type: ignore
    ctx, cache=True, base_image="", update_poetry=False
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param cache: use the cache
    :param update_poetry: run poetry lock to update the packages
    """
    _report_task()
    # Update poetry.
    if update_poetry:
        cmd = "cd devops/docker_build; poetry lock"
        _run(ctx, cmd)
    #
    image_local = get_image("local", base_image)
    #
    _check_image(image_local)
    dockerfile = "devops/docker_build/dev.Dockerfile"
    dockerfile = _to_abs_path(dockerfile)
    #
    opts = "--no-cache" if not cache else ""
    # The container version is the version used from this code.
    container_version = hversi.get_code_version("./version.txt")
    build_tag = _get_build_tag(container_version)
    # TODO(gp): Use _to_multi_line_cmd()
    cmd = rf"""
    DOCKER_BUILDKIT={DOCKER_BUILDKIT} \
    time \
    docker build \
        --progress=plain \
        {opts} \
        --build-arg CONTAINER_VERSION={container_version} \
        --build-arg BUILD_TAG={build_tag} \
        --tag {image_local} \
        --file {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


@task
def docker_tag_local_image_as_dev(ctx, base_image=""):  # type: ignore
    """
    (ONLY CI/CD) Mark the "local" image as "dev".
    """
    _report_task()
    #
    image_local = get_image("local", base_image)
    image_dev = get_image("dev", base_image)
    cmd = f"docker tag {image_local} {image_dev}"
    _run(ctx, cmd)


@task
def docker_push_dev_image(ctx, base_image=""):  # type: ignore
    """
    (ONLY CI/CD) Push the "dev" image to ECR.
    """
    _report_task()
    docker_login(ctx)
    #
    image_dev = get_image("dev", base_image)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd, pty=True)


@task
def docker_release_dev_image(  # type: ignore
    ctx,
    cache=True,
    skip_tests=False,
    run_fast=True,
    run_slow=True,
    run_superslow=False,
    push_to_repo=True,
    update_poetry=False,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the latest "dev" image.

    This can be used to test the entire flow from scratch by building an image,
    running the tests, but not necessarily pushing.

    :param skip_tests: skip all the tests and release the dev image
    :param push_to_repo: push the image to the repo
    :param update_poetry: update package dependencies using poetry
    """
    _report_task()
    # 1) Build "local" image.
    docker_build_local_image(ctx, cache=cache, update_poetry=update_poetry)
    # 2) Run tests for the "local" image.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        run_fast = run_slow = run_superslow = False
    stage = "local"
    if run_fast:
        run_fast_tests(ctx, stage=stage)
    if run_slow:
        run_slow_tests(ctx, stage=stage)
    if run_superslow:
        run_superslow_tests(ctx, stage=stage)
    # 3) Promote the "local" image to "dev".
    docker_tag_local_image_as_dev(ctx)
    # 4) Push the "local" image to ECR.
    if push_to_repo:
        docker_push_dev_image(ctx)
    else:
        _LOG.warning("Skipping pushing dev image to repo, as requested")
    _LOG.info("==> SUCCESS <==")


# PROD image flow:
# - PROD image has no release candidate
# - Start from a DEV image already built and qualified
# - The PROD image is created from the DEV image by copying the code inside the
#   image
# - The PROD image is tagged as "prod"


# TODO(gp): Remove redundancy with docker_build_local_image(), if possible.
@task
def docker_build_prod_image(ctx, cache=True, base_image=""):  # type: ignore
    """
    (ONLY CI/CD) Build a prod image.
    """
    _report_task()
    image_prod = get_image("prod", base_image)
    #
    _check_image(image_prod)
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
        --tag {image_prod} \
        --file {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_prod}"
    _run(ctx, cmd)


@task
def docker_release_prod_image(  # type: ignore
    ctx,
    cache=True,
    skip_tests=False,
    run_fast=True,
    run_slow=True,
    run_superslow=False,
    base_image="",
    push_to_repo=True,
):
    """
    (ONLY CI/CD) Build, test, and release to ECR the prod image.

    Same options as `docker_release_dev_image`.
    """
    _report_task()
    # 1) Build prod image.
    docker_build_prod_image(ctx, cache=cache)
    # 2) Run tests.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        run_fast = run_slow = run_superslow = False
    stage = "prod"
    if run_fast:
        run_fast_tests(ctx, stage=stage)
    if run_slow:
        run_slow_tests(ctx, stage=stage)
    if run_superslow:
        run_superslow_tests(ctx, stage=stage)
    # 3) Push prod image.
    if push_to_repo:
        image_prod = get_image("prod", base_image)
        cmd = f"docker push {image_prod}"
        _run(ctx, cmd, pty=True)
    else:
        _LOG.warning("Skipping pushing image to repo as requested")
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
def run_blank_tests(ctx, stage=STAGE):  # type: ignore
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
    Find test file containing `class_name` and report it in pytest format.

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


# TODO(gp): -> system_interaction.py ?
def _to_pbcopy(txt: str, pbcopy: bool) -> None:
    """
    Save the content of txt in the system clipboard.
    """
    txt = txt.rstrip("\n")
    if not pbcopy:
        print(txt)
        return
    if hsinte.is_running_on_macos():
        # -n = no new line
        cmd = f"echo -n '{txt}' | pbcopy"
        hsinte.system(cmd)
        print(f"\n# Copied to system clipboard:\n{txt}")
    else:
        _LOG.warning("pbcopy works only on macOS")


# TODO(gp): Extend this to accept only the test method.
@task
def find_test_class(ctx, class_name, dir_name=".", pbcopy=True):  # type: ignore
    """
    Report test files containing `class_name` in a format compatible with
    pytest.

    :param class_name: the class to search
    :param dir_name: the dir from which to search (default: .)
    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    _report_task()
    dbg.dassert(class_name != "", "You need to specify a class name")
    _ = ctx
    file_names = _find_test_files(dir_name)
    res = _find_test_class(class_name, file_names)
    res = " ".join(res)
    # Print or copy to clipboard.
    _to_pbcopy(res, pbcopy)


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
    Report test files containing `class_name` in pytest format.

    :param class_name: the class to search
    :param dir_name: the dir from which to search
    """
    _report_task()
    _ = ctx
    dbg.dassert_ne(decorator_name, "", "You need to specify a decorator name")
    file_names = _find_test_files(dir_name)
    res = _find_test_class(decorator_name, file_names)
    res = " ".join(res)
    print(res)


# #############################################################################


@task
def find_check_string_output(  # type: ignore
    ctx, class_name, method_name, as_python=True, fuzzy_match=False, pbcopy=True
):
    """
    Find output of `check_string()` in the test running
    class_name::method_name.

    E.g., for `TestResultBundle::test_from_config1` return the content of the file
        `./core/dataflow/test/TestResultBundle.test_from_config1/output/test.txt`

    :param as_python: if True return the snippet of Python code that replaces the
        `check_string()` with a `assert_equal`
    :param fuzzy_match: if True return Python code with `fuzzy_match=True`
    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    _report_task()
    _ = ctx
    dbg.dassert_ne(class_name, "", "You need to specify a class name")
    dbg.dassert_ne(method_name, "", "You need to specify a method name")
    # Look for the directory named `class_name.method_name`.
    cmd = f"find . -name '{class_name}.{method_name}' -type d"
    # > find . -name "TestResultBundle.test_from_config1" -type d
    # ./core/dataflow/test/TestResultBundle.test_from_config1
    _, txt = hsinte.system_to_string(cmd, abort_on_error=False)
    file_names = txt.split("\n")
    if not txt:
        dbg.dfatal(f"Can't find the requested dir with '{cmd}'")
    if len(file_names) > 1:
        dbg.dfatal(f"Found more than one dir with '{cmd}':\n{txt}")
    dir_name = file_names[0]
    # Find the only file underneath that dir.
    dbg.dassert_dir_exists(dir_name)
    cmd = f"find {dir_name} -name '*.txt' -type f"
    _, file_name = hsinte.system_to_one_line(cmd)
    dbg.dassert_file_exists(file_name)
    # Read the content of the file.
    _LOG.info("Found file '%s' for %s::%s", file_name, class_name, method_name)
    txt = hio.from_file(file_name)
    if as_python:
        # Package the code snippet.
        if not fuzzy_match:
            # Align the output at the same level as 'exp = r...'.
            num_spaces = 8
            txt = hprint.indent(txt, num_spaces=num_spaces)
        output = f"""
        exp = r\"\"\"
{txt}
        \"\"\".lstrip().rstrip()
        self.assert_equal(act, exp, fuzzy_match={fuzzy_match})
        """
    else:
        output = txt
    # Print or copy to clipboard.
    _to_pbcopy(output, pbcopy)
    return output


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
    # TODO(gp): Use _to_multi_line_cmd()
    pytest_opts = " ".join([po.rstrip().lstrip() for po in pytest_opts_tmp])
    cmd = f"pytest {pytest_opts}"
    return cmd


def _run_test_cmd(
    ctx: Any,
    stage: str,
    cmd: str,
    coverage: bool,
    collect_only: bool,
    start_coverage_script: bool,
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
        msg = """
- The coverage results in textual form are above

- To browse the files annotate with coverage, start a server (not from the
  container):
  > (cd ./htmlcov; python -m http.server 33333)
- Then go with your browser to `localhost:33333` to see which code is
  covered
"""
        print(msg)
        if start_coverage_script and hsinte.is_running_on_macos():
            # Create and run a script to show the coverage in the browser.
            script_txt = """(sleep 2; open http://localhost:33333) &
(cd ./htmlcov; python -m http.server 33333)"""
            script_name = "./tmp.coverage.sh"
            hsinte.create_executable_script(script_name, script_txt)
            hsinte.system(script_name)


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
    start_coverage_script: bool = True,
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
    _run_test_cmd(ctx, stage, cmd, coverage, collect_only, start_coverage_script)


# TODO(gp): Pass a test_list in fast, slow, ... instead of duplicating all the code.
@task
def run_fast_tests(  # type: ignore
    ctx,
    stage=STAGE,
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
    stage=STAGE,
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
    stage=STAGE,
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
    stage=STAGE,
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
def traceback(ctx, log_name="", purify=True):  # type: ignore
    """
    Parse the traceback from pytest and navigate it with vim.

    > pyt helpers/test/test_traceback.py
    > invoke traceback
    # There is a also an alias `it` for the previous command line.

    > devops/debug/compare.sh 2>&1 | tee log.txt
    > ie -l log.txt

    :param log_name: the file with the traceback
    """
    _report_task()
    #
    dst_cfile = "cfile"
    hio.delete_file(dst_cfile)
    # Convert the traceback into a cfile.
    cmd = []
    cmd.append("traceback_to_cfile.py")
    if log_name:
        cmd.append(f"-i {log_name}")
    cmd.append(f"-o {dst_cfile}")
    if purify:
        cmd.append("--purify_from_client")
    else:
        cmd.append("--no_purify_from_client")
    cmd = " ".join(cmd)
    _run(ctx, cmd)
    # Read and navigate the cfile with vim.
    if os.path.exists(dst_cfile):
        cmd = 'vim -c "cfile cfile"'
        _run(ctx, cmd, pty=True)
    else:
        _LOG.warning("Can't find %s", dst_file)


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


# TODO(gp): ./dev_scripts/testing/pytest_failed.py

# #############################################################################
# Linter.
# #############################################################################


def _get_lint_docker_cmd(precommit_opts: str, run_bash: bool) -> str:
    superproject_path, submodule_path = git.get_path_from_supermodule()
    if superproject_path:
        # We are running in a Git submodule.
        work_dir = f"/src/{submodule_path}"
        repo_root = superproject_path
    else:
        work_dir = "/src"
        repo_root = os.getcwd()
    _LOG.debug("work_dir=%s repo_root=%s", work_dir, repo_root)
    # TODO(gp): Do not hardwire the repo.
    # image = get_default_param("DEV_TOOLS_IMAGE_PROD")
    image = "665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools:prod"
    # image="665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools:local"
    docker_cmd_ = ["docker run", "--rm"]
    if run_bash:
        docker_cmd_.append("-it")
    else:
        docker_cmd_.append("-t")
    docker_cmd_.extend(
        [f"-v '{repo_root}':/src", f"--workdir={work_dir}", f"{image}"]
    )
    # Build the command inside Docker.
    cmd = f"'pre-commit {precommit_opts}'"
    if run_bash:
        _LOG.warning("Run bash instead of:\n  > %s", cmd)
        cmd = "bash"
    docker_cmd_.append(cmd)
    #
    docker_cmd_ = _to_single_line_cmd(docker_cmd_)
    return docker_cmd_


def _parse_linter_output(txt: str) -> str:
    """
    Parse the output of the linter and return a file suitable for vim quickfix.
    """
    stage: Optional[str] = None
    output: List[str] = []
    for i, line in enumerate(txt.split("\n")):
        _LOG.debug("%d:line='%s'", i + 1, line)
        # Tabs remover...............................................Passed
        # isort......................................................Failed
        # Don't commit to branch...............................^[[42mPassed^[[m
        m = re.search(r"^(\S.*?)\.{10,}\S+?(Passed|Failed)\S*?$", line)
        if m:
            stage = m.group(1)
            result = m.group(2)
            _LOG.debug("  -> stage='%s' (%s)", stage, result)
            continue
        # core/dataflow/nodes.py:601:9: F821 undefined name '_check_col_names'
        m = re.search(r"^(\S+):(\d+)[:\d+:]\s+(.*)$", line)
        if m:
            _LOG.debug("  -> Found a lint to parse: '%s'", line)
            dbg.dassert_is_not(stage, None)
            file_name = m.group(1)
            line_num = int(m.group(2))
            msg = m.group(3)
            _LOG.debug(
                "  -> file_name='%s' line_num=%d msg='%s'",
                file_name,
                line_num,
                msg,
            )
            output.append(f"{file_name}:{line_num}:[{stage}] {msg}")
    # Sort to keep the lints in order of files.
    output = sorted(output)
    output_as_str = "\n".join(output)
    return output_as_str


@task
def lint(  # type: ignore
    ctx,
    modified=False,
    branch=False,
    last_commit=False,
    files="",
    phases="",
    only_format_steps=False,
    # stage="prod",
    run_bash=False,
    run_linter_step=True,
    parse_linter_output=True,
):
    """
    Lint files.

    :param modified: select the files modified in the client
    :param branch: select the files modified in the current branch
    :param last_commit: select the files modified in the previous commit
    :param files: specify a space-separated list of files
    :param phases: specify the lint phases to execute
    :param only_format_steps: run only the formatting steps
    :param run_bash: instead of running pre-commit, run bash to debug
    :param run_linter_step: run linter step
    :param parse_linter_output: parse linter output and generate vim cfile
    """
    _report_task()
    lint_file_name = "linter_output.txt"
    # Remove the file.
    if os.path.exists(lint_file_name):
        cmd = f"rm {lint_file_name}"
        _run(ctx, cmd)
    #
    if only_format_steps:
        dbg.dassert_eq(phases, "")
        phases = "isort black"
    if run_linter_step:
        # We don't want to run this all the times.
        # docker_pull(ctx, stage=stage, images="dev_tools")
        # Get the files to lint.
        # For linting we can use only files modified in the client, in the branch, or
        # specified.
        mutually_exclusive = True
        # pre-commit doesn't handle directories, but only files.
        remove_dirs = True
        files_as_list = _get_files_to_process(
            modified, branch, last_commit, files, mutually_exclusive, remove_dirs
        )
        _LOG.info("Files to lint:\n%s", "\n".join(files_as_list))
        if not files_as_list:
            _LOG.warning("Nothing to lint: exiting")
            return
        files_as_str = " ".join(files_as_list)
        phases = phases.split(" ")
        for phase in phases:
            # Prepare the command line.
            precommit_opts = [
                f"run {phase}",
                "-c /app/.pre-commit-config.yaml",
                f"--files {files_as_str}",
            ]
            precommit_opts = _to_single_line_cmd(precommit_opts)
            # Execute command line.
            cmd = _get_lint_docker_cmd(precommit_opts, run_bash)
            cmd = f"({cmd}) 2>&1 | tee -a {lint_file_name}"
            if run_bash:
                # We don't execute this command since pty=True corrupts the terminal
                # session.
                print("# To get a bash session inside Docker run:")
                print(cmd)
                return
            # Run.
            _run(ctx, cmd)
    else:
        _LOG.warning("Skipping linter step, as per user request")
    #
    if parse_linter_output:
        # Parse the linter output into a cfile.
        _LOG.info("Parsing '%s'", lint_file_name)
        txt = hio.from_file(lint_file_name)
        cfile = _parse_linter_output(txt)
        cfile_name = "./linter_warnings.txt"
        hio.to_file(cfile_name, cfile)
        _LOG.info("Saved cfile in '%s'", cfile_name)
        print(cfile)
    else:
        _LOG.warning("Skipping lint parsing, as per user request")


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


def _get_repo_full_name_from_cmd(repo: str) -> str:
    """
    Convert the `repo` from command line (e.g., "current", "amp", "lem") to the
    repo full name.
    """
    repo_full_name: str
    if repo == "current":
        repo_full_name = git.get_repo_full_name_from_dirname(".")
    else:
        repo_full_name = git.get_repo_name(repo, in_mode="short_name")
    _LOG.debug("repo=%s -> repo_full_name=%s", repo, repo_full_name)
    return repo_full_name


def _get_gh_issue_title(issue_id: int, repo: str) -> str:
    """
    Get the title of a GitHub issue.

    :param repo: `current` refer to the repo where we are, otherwise a repo short
        name (e.g., "amp")
    """
    repo_full_name = _get_repo_full_name_from_cmd(repo)
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
    for char in ": + ( ) / ` *".split():
        title = title.replace(char, "")
    # Replace multiple spaces with one.
    title = re.sub(r"\s+", " ", title)
    #
    title = title.replace(" ", "_")
    # Add the `AmpTaskXYZ_...`
    repo_short_name = git.get_repo_name(repo_full_name, in_mode="full_name")
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
    _LOG.debug("task_prefix=%s", task_prefix)
    title = "%s%d_%s" % (task_prefix, issue_id, title)
    return title


@task
def gh_issue_title(ctx, issue_id, repo="current", pbcopy=True):  # type: ignore
    """
    Print the title that corresponds to the given issue and repo. E.g.,
    AmpTask1251_Update_GH_actions_for_amp.

    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    _report_task()
    _ = ctx
    issue_id = int(issue_id)
    dbg.dassert_lte(1, issue_id)
    res = _get_gh_issue_title(issue_id, repo)
    # Print or copy to clipboard.
    _to_pbcopy(res, pbcopy)


# TODO(gp): Add unit test for
# i gh_create_pr --no-draft --body="Misc changes while adding unit tests"


@task
def gh_create_pr(  # type: ignore
    ctx, body="", draft=True, repo="current", title=""
):
    """
    Create a draft PR for the current branch in the corresponding repo.

    :param body: the body of the PR
    :param draft: draft or ready-to-review PR
    """
    _report_task()
    branch_name = git.get_branch_name()
    if not title:
        # Use the branch name as title.
        title = branch_name
    repo_full_name = _get_repo_full_name_from_cmd(repo)
    _LOG.info(
        "Creating PR with title '%s' for '%s' in %s",
        title,
        branch_name,
        repo_full_name,
    )
    # TODO(gp): Check whether the PR already exists.
    # TODO(gp): Use _to_single_line_cmd
    cmd = (
        "gh pr create"
        + f" --repo {repo_full_name}"
        + (" --draft" if draft else "")
        + f' --title "{title}"'
        + f' --body "{body}"'
    )
    _run(ctx, cmd)
    # TODO(gp): Capture the output of the command and save the info in a
    #  github_current_pr_info:
    # Warning: 22 uncommitted changes
    # Creating pull request for AmpTask1329_Review_code_in_core_04 into master in alphamatic/amp
    # https://github.com/alphamatic/amp/pull/1337


# TODO(gp): Add gh_open_pr to jump to the PR from this branch.

# TODO(gp): Add ./dev_scripts/testing/pytest_count_files.sh
