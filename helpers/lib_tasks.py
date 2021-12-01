"""
Import as:

import helpers.lib_tasks as hlibtask
"""

import datetime
import functools
import glob
import grp
import json
import logging
import os
import pprint
import pwd
import re
import stat
import sys
from typing import Any, Dict, List, Match, Optional, Tuple, Union

import tqdm
from invoke import task

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.
import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.introspection as hintros
import helpers.io_ as hio
import helpers.list as hlist
import helpers.printing as hprint
import helpers.system_interaction as hsysinte
import helpers.table as htable
import helpers.versioning as hversio

_LOG = logging.getLogger(__name__)

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


def get_default_param(key: str) -> Any:
    hdbg.dassert_in(key, _DEFAULT_PARAMS)
    hdbg.dassert_isinstance(key, str)
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


def _report_task(txt: str = "") -> None:
    # On the first invocation report the version.
    global _WAS_FIRST_CALL_DONE
    if not _WAS_FIRST_CALL_DONE:
        _WAS_FIRST_CALL_DONE = True
        hversio.check_version()
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


def _run(ctx: Any, cmd: str, *args: Any, **kwargs: Any) -> None:
    _LOG.debug("cmd=%s", cmd)
    if use_one_line_cmd:
        cmd = _to_single_line_cmd(cmd)
    _LOG.debug("cmd=%s", cmd)
    ctx.run(cmd, *args, **kwargs)


# TODO(gp): We should factor out the meaning of the params in a string and add it
#  to all the tasks' help.
def _get_files_to_process(
    modified: bool,
    branch: bool,
    last_commit: bool,
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
        files = hio.find_all_files(dir_name)
    if files_from_user:
        # If files were passed, overwrite the previous decision.
        files = files_from_user.split(" ")
    # Convert into a list.
    hdbg.dassert_isinstance(files, list)
    files_to_process = [f for f in files if f != ""]
    # We need to remove `amp` to avoid copying the entire tree.
    files_to_process = [f for f in files_to_process if f != "amp"]
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Remove dirs, if needed.
    if remove_dirs:
        files_to_process = hsysinte.remove_dirs(files_to_process)
    _LOG.debug("files_to_process='%s'", str(files_to_process))
    # Ensure that there are files to process.
    if not files_to_process:
        _LOG.warning("No files were selected")
    return files_to_process


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
    var_names = "ECR_BASE_PATH BASE_IMAGE".split()
    for v in var_names:
        print("%s=%s" % (v, get_default_param(v)))


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
    # TODO(gp): Use __file__ instead of hardwiring the file.
    cmd = r'\grep "^@task" -A 1 helpers/lib_tasks.py | grep def'
    # def print_setup(ctx):  # type: ignore
    # def git_pull(ctx):  # type: ignore
    # def git_pull_master(ctx):  # type: ignore
    _, txt = hsysinte.system_to_string(cmd)
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
# Reuse hgit.git_stash_push() and hgit.stash_apply()
# git stash save your-file-name
# git checkout master
# # do whatever you had to do with master
# git checkout staging
# git stash pop


@task
def git_clean(ctx, dry_run=False):  # type: ignore
    """
    Clean the repo_short_name and its submodules from artifacts.

    Run `git status --ignored` to see what it's skipped.
    """
    _report_task(hprint.to_str("dry_run"))
    # TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
    #  mistake.
    # Clean recursively.
    git_clean_cmd = "git clean -fd"
    if dry_run:
        git_clean_cmd += " --dry-run"
    # Clean current repo.
    cmd = git_clean_cmd
    _run(ctx, cmd)
    # Clean submodules.
    cmd = f"git submodule foreach '{git_clean_cmd}'"
    _run(ctx, cmd)
    # Delete other files.
    to_delete = [
        r"*\.pyc",
        r"*\.pyo",
        r".coverage",
        r".ipynb_checkpoints",
        r".mypy_cache",
        r".pytest_cache",
        r"__pycache__",
        r"cfile",
        r"tmp.*",
        r"*.tmp",
    ]
    opts = [f"-name '{opt}'" for opt in to_delete]
    opts = " -o ".join(opts)
    cmd = f"find . {opts} | sort"
    if not dry_run:
        cmd += " | xargs rm -rf"
    _run(ctx, cmd)


@task
def git_add_all_untracked(ctx):  # type: ignore
    """
    Add all untracked files to git.
    """
    _report_task()
    cmd = "git add $(git ls-files -o --exclude-standard)"
    _run(ctx, cmd)


@task
def git_create_patch(  # type: ignore
    ctx, mode="diff", modified=False, branch=False, last_commit=False, files=""
):
    """
    Create a patch file for the entire repo_short_name client from the base
    revision. This script accepts a list of files to package, if specified.

    The parameters `modified`, `branch`, `last_commit` have the same meaning as
    in `_get_files_to_process()`.

    :param mode: what kind of patch to create
        - "diff": (default) creates a patch with the diff of the files
        - "tar": creates a tar ball with all the files
    """
    _report_task(hprint.to_str("mode modified branch last_commit files"))
    _ = ctx
    # TODO(gp): Check that the current branch is up to date with master to avoid
    #  failures when we try to merge the patch.
    hdbg.dassert_in(mode, ("tar", "diff"))
    # For now we just create a patch for the current submodule.
    # TODO(gp): Extend this to handle also nested repos.
    super_module = False
    git_client_root = hgit.get_client_root(super_module)
    hash_ = hgit.get_head_hash(git_client_root, short_hash=True)
    timestamp = _get_ET_timestamp()
    #
    tag = os.path.basename(git_client_root)
    dst_file = f"patch.{tag}.{hash_}.{timestamp}"
    if mode == "tar":
        dst_file += ".tgz"
    elif mode == "diff":
        dst_file += ".patch"
    else:
        hdbg.dfatal("Invalid code path")
    _LOG.debug("dst_file=%s", dst_file)
    # Summary of files.
    _LOG.info(
        "Difference between HEAD and master:\n%s",
        hgit.get_summary_files_in_branch("master", "."),
    )
    # Get the files.
    all_ = False
    # We allow to specify files as a subset of files modified in the branch or
    # in the client.
    mutually_exclusive = False
    # We don't allow to specify directories.
    remove_dirs = True
    files_as_list = _get_files_to_process(
        modified,
        branch,
        last_commit,
        all_,
        files,
        mutually_exclusive,
        remove_dirs,
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
            hdbg.dfatal(
                "You need to specify one among -modified, --branch, "
                "--last-commit"
            )
        cmd = f"git diff {opts} --binary {files_as_str} >{dst_file}"
        cmd_inv = "git apply"
    # Execute patch command.
    _LOG.info("Creating the patch into %s", dst_file)
    hdbg.dassert_ne(cmd, "")
    _LOG.debug("cmd=%s", cmd)
    rc = hsysinte.system(cmd, abort_on_error=False)
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
def git_files(  # type: ignore
    ctx, modified=False, branch=False, last_commit=False, pbcopy=False
):
    """
    Report which files are changed in the current branch with respect to
    master.

    The params have the same meaning as in `_get_files_to_process()`.
    """
    _report_task()
    _ = ctx
    all_ = False
    files = ""
    mutually_exclusive = True
    # pre-commit doesn't handle directories, but only files.
    remove_dirs = True
    files_as_list = _get_files_to_process(
        modified,
        branch,
        last_commit,
        all_,
        files,
        mutually_exclusive,
        remove_dirs,
    )
    print("\n".join(sorted(files_as_list)))
    if pbcopy:
        res = " ".join(files_as_list)
        _to_pbcopy(res, pbcopy)


@task
def git_last_commit_files(ctx, pbcopy=True):  # type: ignore
    """
    Print the status of the files in the previous commit.

    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    cmd = 'git log -1 --name-status --pretty=""'
    _run(ctx, cmd)
    # Get the list of existing files.
    files = hgit.get_previous_committed_files(".")
    txt = "\n".join(files)
    print(f"\n# The files modified are:\n{txt}")
    # Save to clipboard.
    res = " ".join(files)
    _to_pbcopy(res, pbcopy)


# Branches workflows


@task
def git_branch_files(ctx):  # type: ignore
    """
    Report which files are added, changed, modified in the current branch with
    respect to master.
    """
    _report_task()
    _ = ctx
    print(
        "Difference between HEAD and master:\n"
        + hgit.get_summary_files_in_branch("master", ".")
    )


@task
def git_create_branch(  # type: ignore
    ctx,
    branch_name="",
    issue_id=0,
    repo_short_name="current",
    suffix="",
    only_branch_from_master=True,
):
    """
    Create and push upstream branch `branch_name` or the one corresponding to
    `issue_id` in repo_short_name `repo_short_name`.

    E.g.,
    ```
    > git checkout -b LemTask169_Get_GH_actions
    > git push --set- upstream origin LemTask169_Get_GH_actions
    ```

    :param branch_name: name of the branch to create (e.g.,
        `LemTask169_Get_GH_actions`)
    :param issue_id: use the canonical name for the branch corresponding to that
        issue
    :param repo_short_name: name of the GitHub repo_short_name that the `issue_id` belongs to
        - "current" (default): the current repo_short_name
        - short name (e.g., "amp", "lm") of the branch
    :param suffix: suffix (e.g., "02") to add to the branch name when using issue_id
    :param only_branch_from_master: only allow to branch from master
    """
    _report_task()
    if issue_id > 0:
        # User specified an issue id on GitHub.
        hdbg.dassert_eq(
            branch_name, "", "You can't specify both --issue and --branch_name"
        )
        title, _ = _get_gh_issue_title(issue_id, repo_short_name)
        branch_name = title
        _LOG.info(
            "Issue %d in %s repo_short_name corresponds to '%s'",
            issue_id,
            repo_short_name,
            branch_name,
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
    hdbg.dassert_ne(branch_name, "")
    # Make sure we are branching from `master`, unless that's what the user wants.
    curr_branch = hgit.get_branch_name()
    if curr_branch != "master":
        if only_branch_from_master:
            hdbg.dfatal(
                "You should branch from master and not from '%s'" % curr_branch
            )
    # Fetch master.
    cmd = "git pull --autostash"
    _run(ctx, cmd)
    # git checkout -b LmTask169_Get_GH_actions_working_on_lm
    cmd = f"git checkout -b {branch_name}"
    _run(ctx, cmd)
    # TODO(gp): If the branch already exists, increase the number.
    #   git checkout -b AmpTask1329_Review_code_in_core_03
    #   fatal: A branch named 'AmpTask1329_Review_code_in_core_03' already exists.
    #   saggese@gpmaclocal.local ==> RC: 128 <==
    cmd = f"git push --set-upstream origin {branch_name}"
    _run(ctx, cmd)


def _delete_branches(ctx: Any, tag: str, confirm_delete: bool) -> None:
    if tag == "local":
        # Delete local branches that are already merged into master.
        # > git branch --merged
        # * AmpTask1251_Update_GH_actions_for_amp_02
        find_cmd = r"git branch --merged master | grep -v master | grep -v \*"
        delete_cmd = "git branch -d"
    elif tag == "remote":
        # Get the branches to delete.
        find_cmd = (
            "git branch -r --merged origin/master"
            + r" | grep -v master | sed 's/origin\///'"
        )
        delete_cmd = "git push origin --delete"
    else:
        raise ValueError(f"Invalid tag='{tag}'")
    # TODO(gp): Use system_to_lines
    _, txt = hsysinte.system_to_string(find_cmd, abort_on_error=False)
    branches = hsysinte.text_to_list(txt)
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
        hsysinte.query_yes_no(
            hdbg.WARNING + f": Delete these {tag} branches?", abort_on_no=True
        )
    for branch in branches:
        cmd_tmp = f"{delete_cmd} {branch}"
        _run(ctx, cmd_tmp)


@task
def git_delete_merged_branches(ctx, confirm_delete=True):  # type: ignore
    """
    Remove (both local and remote) branches that have been merged into master.
    """
    _report_task()
    hdbg.dassert(
        hgit.get_branch_name(),
        "master",
        "You need to be on master to delete dead branches",
    )
    #
    cmd = "git fetch --all --prune"
    _run(ctx, cmd)
    # Delete local and remote branches that are already merged into master.
    _delete_branches(ctx, "local", confirm_delete)
    _delete_branches(ctx, "remote", confirm_delete)
    #
    cmd = "git fetch --all --prune"
    _run(ctx, cmd)


@task
def git_rename_branch(ctx, new_branch_name):  # type: ignore
    """
    Rename current branch both locally and remotely.
    """
    _report_task()
    #
    old_branch_name = hgit.get_branch_name(".")
    hdbg.dassert_ne(old_branch_name, new_branch_name)
    msg = (
        f"Do you want to rename the current branch '{old_branch_name}' to "
        f"'{new_branch_name}'"
    )
    hsysinte.query_yes_no(msg, abort_on_no=True)
    # https://stackoverflow.com/questions/6591213/how-do-i-rename-a-local-git-branch
    # To rename a local branch:
    # git branch -m <oldname> <newname>
    cmd = f"git branch -m {new_branch_name}"
    _run(ctx, cmd)
    # git push origin -u <newname>
    cmd = f"git push origin {new_branch_name}"
    _run(ctx, cmd)
    # git push origin --delete <oldname>
    cmd = f"git push origin --delete {old_branch_name}"
    _run(ctx, cmd)
    print("Done")


# TODO(gp): Add the following scripts:
# dev_scripts/git/git_backup.sh
# dev_scripts/git/gcl
# dev_scripts/git/git_branch.sh
# dev_scripts/git/git_branch_point.sh
# dev_scripts/create_class_diagram.sh

# #############################################################################
# Basic Docker commands.
# #############################################################################


@task
def docker_images_ls_repo(ctx):  # type: ignore
    """
    List images in the logged in repo_short_name.
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
    cmd = f"docker ps --format='{fmt}'"
    cmd = _to_single_line_cmd(cmd)
    _run(ctx, cmd)


def _get_last_container_id() -> str:
    # Get the last started container.
    cmd = "docker ps -l | grep -v 'CONTAINER ID'"
    # CONTAINER ID   IMAGE          COMMAND                  CREATED
    # 90897241b31a   eeb33fe1880a   "/bin/sh -c '/bin/ba…"   34 hours ago ...
    _, txt = hsysinte.system_to_one_line(cmd)
    # Parse the output: there should be at least one line.
    hdbg.dassert_lte(1, len(txt.split(" ")), "Invalid output='%s'", txt)
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
    _, txt = hsysinte.system_to_string(cmd)
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
        hdbg.dassert_lte(
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


@task
def docker_pull(ctx, stage="dev", images="all"):  # type: ignore
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
            image = get_image(base_image, stage, None)
        elif token == "dev_tools":
            image = get_default_param("DEV_TOOLS_IMAGE_PROD")
        else:
            raise ValueError("Can't recognize image token '%s'" % token)
        _LOG.info("token='%s': image='%s'", token, image)
        _dassert_is_image_name_valid(image)
        cmd = f"docker pull {image}"
        _run(ctx, cmd, pty=True)


@functools.lru_cache()
def _get_aws_cli_version() -> int:
    # > aws --version
    # aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49
    # aws-cli/1.20.1 Python/3.9.5 Darwin/19.6.0 botocore/1.20.106
    cmd = "aws --version"
    res = hsysinte.system_to_one_line(cmd)[1]
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
    if hsysinte.is_inside_ci():
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
        ecr_base_path = get_default_param("ECR_BASE_PATH")
        cmd = (
            f"docker login -u AWS -p $(aws ecr get-login --region {region}) "
            + f"https://{ecr_base_path}"
        )
    # cmd = ("aws ecr get-login-password" +
    #       " | docker login --username AWS --password-stdin "
    _run(ctx, cmd)


def get_base_docker_compose_path() -> str:
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
    path, _ = hgit.get_path_from_supermodule()
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


_IMAGE_VERSION_RE = r"\d+\.\d+\.\d+"


def _dassert_is_version_valid(version: str) -> None:
    """
    A valid version looks like: `1.0.0`.
    """
    hdbg.dassert_isinstance(version, str)
    hdbg.dassert_ne(version, "")
    regex = rf"^({_IMAGE_VERSION_RE})$"
    _LOG.debug("Testing with regex='%s'", regex)
    m = re.match(regex, version)
    hdbg.dassert(m, "Invalid version: '%s'", version)


_INTERNET_ADDRESS_RE = r"([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}"
_IMAGE_BASE_NAME_RE = r"[a-z0-9_-]+"
_IMAGE_USER_RE = r"[a-z0-9_-]+"
_IMAGE_STAGE_RE = rf"(local(?:-{_IMAGE_USER_RE})?|dev|prod)"


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
    A base image should look like.

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
            get_default_param("ECR_BASE_PATH")
            + "/"
            + get_default_param("BASE_IMAGE")
        )
    _dassert_is_base_image_name_valid(base_image)
    return base_image


def get_git_tag(
    version: str,
) -> str:
    """
    Return the tag to be used in Git that consists of an image name and
    version.

    :param version: e.g., `1.0.0`. If None, the latest version is used
    :return: e.g., `amp-1.0.0`
    """
    hdbg.dassert_is_not(version, None)
    _dassert_is_version_valid(version)
    base_image = get_default_param("BASE_IMAGE")
    tag_name = f"{base_image}-{version}"
    return tag_name


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
        user = hsysinte.get_user_name()
        image.append(f"-{user}")
    # Handle the version.
    if version is not None and version != "":
        _dassert_is_version_valid(version)
        image.append(f"-{version}")
    #
    image = "".join(image)
    _dassert_is_image_name_valid(image)
    return image


def _get_docker_cmd(
    base_image: str,
    stage: str,
    version: str,
    cmd: str,
    *,
    extra_env_vars: Optional[List[str]] = None,
    extra_docker_compose_files: Optional[List[str]] = None,
    extra_docker_run_opts: Optional[List[str]] = None,
    service_name: str = "app",
    entrypoint: bool = True,
    as_user: bool = True,
    print_docker_config: bool = False,
) -> str:
    """
    :param base_image, stage, version: like in `get_image()`
    :param cmd: command to run inside Docker container
    :param as_user: pass the user / group id or not
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
    # - Handle the docker compose files.
    dir_name = hgit.get_repo_full_name_from_dirname(".", include_host_name=False)
    repo_short_name = hgit.get_repo_name(dir_name, in_mode="full_name")
    _LOG.debug("repo_short_name=%s", repo_short_name)
    #
    docker_compose_files = []
    if has_default_param("USE_ONLY_ONE_DOCKER_COMPOSE"):
        # Use only one docker compose file.
        # TODO(gp): Hacky fix for CmampTask386.
        if repo_short_name == "amp":
            docker_compose_file_tmp = _get_amp_docker_compose_path()
        else:
            docker_compose_file_tmp = get_base_docker_compose_path()
        docker_compose_files.append(docker_compose_file_tmp)
    else:
        # Use one or two docker compose files.
        docker_compose_files.append(get_base_docker_compose_path())
        if repo_short_name == "amp":
            docker_compose_file_tmp = _get_amp_docker_compose_path()
            if docker_compose_file_tmp:
                docker_compose_files.append(docker_compose_file_tmp)
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
        hdbg.dassert_exists(docker_compose)
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
    # Based on AmpTask1864 it seems that we need to use root in the CI to be
    # able to log in GH touching $HOME/.config/gh.
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
            hsysinte.system_to_string(docker_config_cmd_as_str)[1],
        )
    # Print the config for debugging purpose.
    docker_cmd_ = _to_multi_line_cmd(docker_cmd_)
    return docker_cmd_


def _docker_cmd(
    ctx: Any,
    docker_cmd_: str,
) -> None:
    """
    Execute a docker command printing the command.
    """
    _LOG.debug("cmd=%s", docker_cmd_)
    _run(ctx, docker_cmd_, pty=True)


@task
def docker_bash(  # type: ignore
    ctx,
    base_image="",
    stage="dev",
    version="",
    entrypoint=True,
    as_user=True,
):
    """
    Start a bash shell inside the container corresponding to a stage.
    """
    _report_task()
    cmd = "bash"
    docker_cmd_ = _get_docker_cmd(
        base_image, stage, version, cmd, entrypoint=entrypoint, as_user=as_user
    )
    _docker_cmd(ctx, docker_cmd_)


@task
def docker_cmd(  # type: ignore
    ctx, base_image="", stage="dev", version="", cmd=""
):
    """
    Execute the command `cmd` inside a container corresponding to a stage.
    """
    _report_task()
    hdbg.dassert_ne(cmd, "")
    # TODO(gp): Do we need to overwrite the entrypoint?
    docker_cmd_ = _get_docker_cmd(base_image, stage, version, cmd)
    _docker_cmd(ctx, docker_cmd_)


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
    docker_cmd_ = _get_docker_cmd(
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
    port=9999,
    self_test=False,
):
    """
    Run jupyter notebook server.

    :param auto_assign_port: use the UID of the user and the inferred number of the
        repo (e.g., 4 for `~/src/amp4`) to get a unique port
    """
    _report_task()
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
        _LOG.info("Assigned port is %s", port)
    #
    print_docker_config = False
    docker_cmd_ = _get_docker_jupyter_cmd(
        base_image, stage, version, port, self_test,
        print_docker_config=print_docker_config
    )
    _docker_cmd(ctx, docker_cmd_)


# #############################################################################
# Docker image workflows.
# #############################################################################


def _to_abs_path(filename: str) -> str:
    filename = os.path.abspath(filename)
    hdbg.dassert_exists(filename)
    return filename


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
):
    """
    Build a local image (i.e., a release candidate "dev" image).

    :param version: version to tag the image and code with
    :param cache: use the cache
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    :param stage: select a specific stage for the Docker image
    :param update_poetry: run poetry lock to update the packages
    """
    _report_task()
    _dassert_is_version_valid(version)
    # Update poetry.
    if update_poetry:
        cmd = "cd devops/docker_build; poetry lock -v"
        _run(ctx, cmd)
    #
    image_local = get_image(base_image, "local", version)
    #
    _dassert_is_image_name_valid(image_local)
    #
    git_tag_prefix = get_default_param("BASE_IMAGE")
    # Tag the container with the current version of 
    # the code to keep them in sync.
    container_version = get_git_tag(version)
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
        --build-arg AM_CONTAINER_VERSION={container_version} \
        --build-arg AM_IMAGE_NAME={git_tag_prefix} \
        --tag {image_local} \
        --file {dockerfile} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_local}"
    _run(ctx, cmd)


@task
def docker_tag_local_image_as_dev(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Mark the "local" image as "dev".

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task()
    _dassert_is_version_valid(version)
    # Tag local image as versioned dev image (e.g., `dev-1.0.0`).
    image_versioned_local = get_image(base_image, "local", version)
    image_versioned_dev = get_image(base_image, "dev", version)
    cmd = f"docker tag {image_versioned_local} {image_versioned_dev}"
    _run(ctx, cmd)
    # Tag local image as dev image.
    image_dev = get_image(base_image, "dev", None)
    cmd = f"docker tag {image_versioned_local} {image_dev}"
    _run(ctx, cmd)
    # Tag the Git repo with the tag corresponding to the image, e.g., `amp-1.0.0`.
    # TODO(gp): Should we add also the stage in the same format `amp:dev-1.0.0`?
    tag_name = get_git_tag(version)
    hgit.git_tag(tag_name)


@task
def docker_push_dev_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the "dev" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task()
    _dassert_is_version_valid(version)
    #
    docker_login(ctx)
    # Push Docker versioned tag.
    image_versioned_dev = get_image(base_image, "dev", version)
    cmd = f"docker push {image_versioned_dev}"
    _run(ctx, cmd, pty=True)
    # Push Docker tag.
    image_dev = get_image(base_image, "dev", None)
    cmd = f"docker push {image_dev}"
    _run(ctx, cmd, pty=True)
    # Push Git tag.
    tag_name = get_git_tag(version)
    hgit.git_push_tag(tag_name)


@task
def docker_release_dev_image(  # type: ignore
    ctx,
    version,
    cache=True,
    skip_tests=False,
    fast_tests=True,
    slow_tests=True,
    superslow_tests=False,
    qa_tests=True,
    push_to_repo=True,
    update_poetry=False,
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
    _report_task()
    # 1) Build "local" image.
    docker_build_local_image(
        ctx,
        cache=cache,
        update_poetry=update_poetry,
        version=version,
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
        run_fast_tests(stage=stage)
    if slow_tests:
        run_slow_tests(stage=stage)
    if superslow_tests:
        run_superslow_tests(stage=stage)
    # 3) Promote the "local" image to "dev".
    docker_tag_local_image_as_dev(ctx)
    # 4) Run QA tests for the (local version) of the dev image.
    if qa_tests:
        qa_test_fn = get_default_param("END_TO_END_TEST_FN")
        if not qa_test_fn(ctx, stage="dev"):
            msg = "End-to-end test has failed"
            _LOG.error(msg)
            raise RuntimeError(msg)
    # 5) Push the "dev" image to ECR.
    if push_to_repo:
        docker_push_dev_image(ctx)
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
):
    """
    (ONLY CI/CD) Build a prod image.

    Phases:
    - Build the prod image on top of the dev image

    :param version: version to tag the image and code with
    :param cache: note that often the prod image is just a copy of the dev
        image so caching makes no difference
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task()
    _dassert_is_version_valid(version)
    image_prod = get_image(base_image, "prod", version)
    #
    _dassert_is_image_name_valid(image_prod)
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
        --build-arg VERSION={version} \
        .
    """
    _run(ctx, cmd)
    #
    cmd = f"docker image ls {image_prod}"
    _run(ctx, cmd)


@task
def docker_push_prod_image(  # type: ignore
    ctx,
    version,
    base_image="",
):
    """
    (ONLY CI/CD) Push the "prod" image to ECR.

    :param version: version to tag the image and code with
    :param base_image: e.g., *****.dkr.ecr.us-east-1.amazonaws.com/amp
    """
    _report_task()
    docker_login(ctx)
    #
    image_prod = get_image(base_image, "prod", None)
    cmd = f"docker push {image_prod}"
    _run(ctx, cmd, pty=True)
    #
    # Push versioned tag.
    image_versioned_prod = get_image(base_image, "prod", version)
    cmd = f"docker push {image_versioned_prod}"
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
    _report_task()
    # 1) Build prod image.
    docker_build_prod_image(ctx, cache=cache, version=version)
    # 2) Run tests.
    if skip_tests:
        _LOG.warning("Skipping all tests and releasing")
        fast_tests = slow_tests = superslow_tests = False
    stage = "prod"
    if fast_tests:
        run_fast_tests(stage=stage, version=version)
    if slow_tests:
        run_slow_tests(stage=stage, version=version)
    if superslow_tests:
        run_superslow_tests(stage=stage, version=version)
    # 3) Push prod image.
    if push_to_repo:
        docker_push_prod_image(ctx, version=version)
    else:
        _LOG.warning("Skipping pushing image to repo_short_name as requested")
    _LOG.info("==> SUCCESS <==")


@task
def docker_release_all(ctx, version):  # type: ignore
    """
    (ONLY CI/CD) Release both dev and prod image to ECR.

    This includes:
    - docker_release_dev_image
    - docker_release_prod_image

    :param version: version to tag the image and code with
    """
    _report_task()
    docker_release_dev_image(ctx, version)
    docker_release_prod_image(ctx, version)
    _LOG.info("==> SUCCESS <==")


# #############################################################################
# Find test.
# #############################################################################


def _find_test_files(
    dir_name: Optional[str] = None, use_absolute_path: bool = False
) -> List[str]:
    """
    Find all the files containing test code in `dir_name`.
    """
    dir_name = dir_name or "."
    hdbg.dassert_dir_exists(dir_name)
    _LOG.debug("dir_name=%s", dir_name)
    # Find all the file names containing test code.
    _LOG.info("Searching from '%s'", dir_name)
    path = os.path.join(dir_name, "**", "test_*.py")
    _LOG.debug("path=%s", path)
    file_names = glob.glob(path, recursive=True)
    _LOG.debug("Found %d files: %s", len(file_names), str(file_names))
    hdbg.dassert_no_duplicates(file_names)
    # Test files should always under a dir called `test`.
    for file_name in file_names:
        if "/old/" in file_name:
            continue
        hdbg.dassert_eq(
            os.path.basename(os.path.dirname(file_name)),
            "test",
            "Test file '%s' needs to be under a `test` dir ",
            file_name,
        )
        hdbg.dassert_not_in(
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
    hdbg.dassert_no_duplicates(file_names)
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
    if not txt:
        print("Nothing to copy")
        return
    if hsysinte.is_running_on_macos():
        # -n = no new line
        cmd = f"echo -n '{txt}' | pbcopy"
        hsysinte.system(cmd)
        print(f"\n# Copied to system clipboard:\n{txt}")
    else:
        _LOG.warning("pbcopy works only on macOS")
        print(txt)


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
    _report_task("class_name dir_name pbcopy")
    hdbg.dassert(class_name != "", "You need to specify a class name")
    _ = ctx
    file_names = _find_test_files(dir_name)
    res = _find_test_class(class_name, file_names)
    res = " ".join(res)
    # Print or copy to clipboard.
    _to_pbcopy(res, pbcopy)


# #############################################################################
# Find test decorator.
# #############################################################################


# TODO(gp): decorator_name -> pytest_mark
def _find_test_decorator(decorator_name: str, file_names: List[str]) -> List[str]:
    """
    Find test files containing tests with a certain decorator
    `@pytest.mark.XYZ`.
    """
    hdbg.dassert_isinstance(file_names, list)
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
    hdbg.dassert_ne(decorator_name, "", "You need to specify a decorator name")
    file_names = _find_test_files(dir_name)
    res = _find_test_class(decorator_name, file_names)
    res = " ".join(res)
    print(res)


# #############################################################################
# Find / replace `check_string`.
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
    hdbg.dassert_ne(class_name, "", "You need to specify a class name")
    hdbg.dassert_ne(method_name, "", "You need to specify a method name")
    # Look for the directory named `class_name.method_name`.
    cmd = f"find . -name '{class_name}.{method_name}' -type d"
    # > find . -name "TestResultBundle.test_from_config1" -type d
    # ./core/dataflow/test/TestResultBundle.test_from_config1
    _, txt = hsysinte.system_to_string(cmd, abort_on_error=False)
    file_names = txt.split("\n")
    if not txt:
        hdbg.dfatal(f"Can't find the requested dir with '{cmd}'")
    if len(file_names) > 1:
        hdbg.dfatal(f"Found more than one dir with '{cmd}':\n{txt}")
    dir_name = file_names[0]
    # Find the only file underneath that dir.
    hdbg.dassert_dir_exists(dir_name)
    cmd = f"find {dir_name} -name 'test.txt' -type f"
    _, file_name = hsysinte.system_to_one_line(cmd)
    hdbg.dassert_file_exists(file_name)
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
        act =
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
_TEST_TIMEOUTS_IN_SECS = {
    "fast_tests": 5,
    "slow_tests": 30,
    "superslow_tests": 60 * 60,
}


@task
def run_blank_tests(stage="dev", version=""):  # type: ignore
    """
    (ONLY CI/CD) Test that pytest in the container works.
    """
    _report_task()
    base_image = ""
    cmd = '"pytest -h >/dev/null"'
    docker_cmd_ = _get_docker_cmd(base_image, stage, version, cmd)
    hsysinte.system(docker_cmd_, abort_on_error=False, suppress_output=False)


def _build_run_command_line(
    single_test_type: str,
    pytest_opts: str,
    skip_submodules: bool,
    coverage: bool,
    collect_only: bool,
    tee_to_file: bool,
    # Different params than the `run_*_tests()`.
) -> str:
    """
    Build the pytest run command.

    Same params as `run_fast_tests()`.

    The invariant is that we don't want to duplicate pytest options that can be
    passed by the user through `-p` (unless really necessary).
    """
    hdbg.dassert_in(
        single_test_type,
        _TEST_TIMEOUTS_IN_SECS,
        "Invalid `single_test_type``='%s'",
        single_test_type,
    )
    pytest_opts = pytest_opts or "."
    #
    pytest_opts_tmp = []
    if pytest_opts:
        pytest_opts_tmp.append(pytest_opts)
    skipped_tests = _select_tests_to_skip(single_test_type)
    pytest_opts_tmp.insert(0, f'-m "{skipped_tests}"')
    timeout_in_sec = _TEST_TIMEOUTS_IN_SECS[single_test_type]
    pytest_opts_tmp.append(f"--timeout {timeout_in_sec}")
    if skip_submodules:
        submodule_paths = hgit.get_submodule_paths()
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
    if tee_to_file:
        cmd += f" 2>&1 | tee tmp.pytest.{single_test_type}.log"
    return cmd


def _select_tests_to_skip(single_test_type: str) -> str:
    if single_test_type == "fast_tests":
        skipped_tests = "not slow and not superslow"
    elif single_test_type == "slow_tests":
        skipped_tests = "slow and not superslow"
    elif single_test_type == "superslow_tests":
        skipped_tests = "not slow and superslow"
    else:
        raise ValueError(f"Invalid `single_test_type`={single_test_type}")
    return skipped_tests


def _run_test_cmd(
    stage: str,
    version: str,
    cmd: str,
    coverage: bool,
    collect_only: bool,
    start_coverage_script: bool,
) -> None:
    if collect_only:
        # Clean files.
        hsysinte.system("rm -rf ./.coverage*")
    # Run.
    base_image = ""
    # We need to add some " to pass the string as it is to the container.
    cmd = f"'{cmd}'"
    docker_cmd_ = _get_docker_cmd(base_image, stage, version, cmd)
    _LOG.info("cmd=%s", docker_cmd_)
    hsysinte.system(docker_cmd_, abort_on_error=False, suppress_output=False)
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
        if start_coverage_script and hsysinte.is_running_on_macos():
            # Create and run a script to show the coverage in the browser.
            script_txt = """(sleep 2; open http://localhost:33333) &
(cd ./htmlcov; python -m http.server 33333)"""
            script_name = "./tmp.coverage.sh"
            hsysinte.create_executable_script(script_name, script_txt)
            hsysinte.system(script_name)


def _run_tests(
    stage: str,
    test_type: str,
    version: str,
    pytest_opts: str,
    skip_submodules: bool,
    coverage: bool,
    collect_only: bool,
    tee_to_file: bool,
    *,
    start_coverage_script: bool = True,
) -> None:
    # Build the command line.
    cmd = _build_run_command_line(
        test_type,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )
    # Execute the command line.
    _run_test_cmd(
        stage, version, cmd, coverage, collect_only, start_coverage_script
    )


# TODO(gp): Pass a test_list in fast, slow, ... instead of duplicating all the code.
@task
def run_fast_tests(  # type: ignore
    stage="dev",
    version="",
    pytest_opts="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
    tee_to_file=False,
):
    """
    Run fast tests.

    :param stage: select a specific stage for the Docker image
    :param pytest_opts: option for pytest
    :param skip_submodules: ignore all the dir inside a submodule
    :param coverage: enable coverage computation
    :param collect_only: do not run tests but show what will be executed
    :param tee_to_file: save output of pytest in `tmp.pytest.log`
    """
    _report_task()
    _run_tests(
        stage,
        "fast_tests",
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )


@task
def run_slow_tests(  # type: ignore
    stage="dev",
    version="",
    pytest_opts="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
    tee_to_file=False,
):
    """
    Run slow tests.

    Same params as `invoke run_fast_tests`.
    """
    _report_task()
    _run_tests(
        stage,
        "slow_tests",
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )


@task
def run_superslow_tests(  # type: ignore
    stage="dev",
    version="",
    pytest_opts="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
    tee_to_file=False,
):
    """
    Run superslow tests.

    Same params as `invoke run_fast_tests`.
    """
    _report_task()
    _run_tests(
        stage,
        "superslow_tests",
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )


@task
def run_fast_slow_tests(  # type: ignore
    stage="dev",
    version="",
    pytest_opts="",
    skip_submodules=False,
    coverage=False,
    collect_only=False,
    tee_to_file=False,
):
    """
    Run fast and slow tests.

    Same params as `invoke run_fast_tests`.
    """
    _report_task()
    run_fast_tests(
        stage,
        "fast_tests",
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )
    run_slow_tests(
        stage,
        "slow_tests",
        version,
        pytest_opts,
        skip_submodules,
        coverage,
        collect_only,
        tee_to_file,
    )


# #############################################################################
# Pytest helpers.
# #############################################################################


# TODO(gp): Consolidate the code from dev_scripts/testing here.


@task
def traceback(ctx, log_name="tmp.pytest_script.log", purify=True):  # type: ignore
    """
    Parse the traceback from Pytest and navigate it with vim.

    ```
    # Run a unit test.
    > pytest helpers/test/test_traceback.py 2>&1 | tee tmp.pytest.log
    > pytest.sh helpers/test/test_traceback.py
    # Parse the traceback
    > invoke traceback -i tmp.pytest.log
    ```

    :param log_name: the file with the traceback
    :param purify: purify the filenames from client (e.g., from running inside Docker)
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
    # Purify the file names.
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
        _LOG.warning("Can't find %s", dst_cfile)


@task
def pytest_clean(ctx):  # type: ignore
    """
    Clean pytest artifacts.
    """
    _report_task()
    _ = ctx
    import helpers.pytest_ as hpytest

    hpytest.pytest_clean(".")


@task
def pytest_failed_freeze_test_list(ctx, confirm=False):  # type: ignore
    """
    Copy last list of failed tests to not overwrite with successive pytest
    runs.
    """
    _report_task()
    dir_name = "."
    pytest_failed_tests_file = os.path.join(
        dir_name, ".pytest_cache/v/cache/lastfailed"
    )
    frozen_failed_tests_file = "tmp.pytest_cache.lastfailed"
    if os.path.exists(frozen_failed_tests_file) and not confirm:
        hdbg.dfatal(
            "File {frozen_failed_tests_file} already exists. Re-run with --confirm to overwrite"
        )
    _LOG.info(
        "Copying '%s' to '%s'", pytest_failed_tests_file, frozen_failed_tests_file
    )
    # Make a copy of the pytest file.
    hdbg.dassert_file_exists(pytest_failed_tests_file)
    cmd = f"cp {pytest_failed_tests_file} {frozen_failed_tests_file}"
    _run(ctx, cmd)


def _get_failed_tests_from_file(file_name: str) -> List[str]:
    hdbg.dassert_file_exists(file_name)
    # {
    # "vendors/test/test_vendors.py::Test_gp::test1": true,
    # "vendors/test/test_vendors.py::Test_kibot_utils1::...": true,
    # }
    txt = hio.from_file(file_name)
    vals = json.loads(txt)
    hdbg.dassert_isinstance(vals, dict)
    tests = [k for k, v in vals.items() if v]
    return tests


def _get_failed_tests_from_clipboard() -> List[str]:
    # pylint: disable=line-too-long
    """
    ```

    FAILED core/dataflow/nodes/test/test_sources.py::TestRealTimeDataSource1::test_replayed_real_time1 - TypeError: __init__() got an unexpected keyword argument 'speed_up_factor'
    FAILED helpers/test/test_lib_tasks.py::Test_find_check_string_output1::test1 - TypeError: check_string() takes 2 positional arguments but 3 were given
    FAILED helpers/test/test_lib_tasks.py::Test_find_check_string_output1::test2 - TypeError: check_string() takes 2 positional arguments but 3 were given
    FAILED core/dataflow/test/test_runners.py::TestRealTimeDagRunner1::test1 - TypeError: execute_with_real_time_loop() got an unexpected keyword argument 'num_iterations'
    FAILED helpers/test/test_unit_test.py::TestCheckDataFrame1::test_check_df_not_equal1 - NameError: name 'dedent' is not defined
    FAILED helpers/test/test_unit_test.py::TestCheckDataFrame1::test_check_df_not_equal2 - NameError: name 'dedent' is not defined
    FAILED helpers/test/test_unit_test.py::TestCheckDataFrame1::test_check_df_not_equal4 - NameError: name 'dedent' is not defined
    ```
    """
    # pylint: enable=line-too-long
    hsysinte.system_to_string("pbpaste")
    # TODO(gp): Finish this.
    return []


@task
def pytest_failed(  # type: ignore
    ctx,
    use_frozen_list=True,
    target_type="tests",
    file_name="",
    refresh=False,
    pbcopy=True,
):
    """
    Process the list of failed tests from a pytest run.

    The workflow is:
    ```
    # Run a lot of tests, e.g., the entire regression suite.
    > pytest ...
    # Some tests fail.

    # Freeze the output of pytest so we can re-run only some of them.
    > invoke pytest_failed_freeze_test_list
    #
    > invoke pytest_failed
    ```

    :param use_frozen_list: use the frozen list (default) or the one generated by
        pytest
    :param target_type: specify what to print about the tests
        - tests (default): print the tests in a single line
        - files: print the name of the files containing files
        - classes: print the name of all classes
    :param file_name: specify the file name containing the pytest file to parse
    :param refresh: force to update the frozen file from the current pytest file
    """
    _report_task()
    _ = ctx
    if refresh:
        pytest_failed_freeze_test_list(ctx, confirm=True)
    # Read file.
    if not file_name:
        dir_name = "."
        pytest_failed_tests_file = os.path.join(
            dir_name, ".pytest_cache/v/cache/lastfailed"
        )
        frozen_failed_tests_file = "tmp.pytest_cache.lastfailed"
        if use_frozen_list:
            if os.path.exists(pytest_failed_tests_file) and not os.path.exists(
                frozen_failed_tests_file
            ):
                _LOG.warning("Freezing the pytest outcomes")
                pytest_failed_freeze_test_list(ctx)
            file_name = frozen_failed_tests_file
        else:
            file_name = pytest_failed_tests_file
    _LOG.info("Reading file_name='%s'", file_name)
    hdbg.dassert_file_exists(file_name)
    # E.g., vendors/test/test_vendors.py::Test_gp::test1
    _LOG.info("Reading failed tests from file '%s'", file_name)
    tests = _get_failed_tests_from_file(file_name)
    _LOG.debug("tests=%s", str(tests))
    # Process the tests.
    targets = []
    for test in tests:
        data = test.split("::")
        hdbg.dassert_lte(len(data), 3, "Can't parse '%s'", test)
        # E.g., dev_scripts/testing/test/test_run_tests.py
        # E.g., helpers/test/helpers/test/test_list.py::Test_list_1
        # E.g., core/dataflow/nodes/test/test_volatility_models.py::TestSmaModel::test5
        file_name = test_class = test_method = ""
        if len(data) >= 1:
            file_name = data[0]
        if len(data) >= 2:
            test_class = data[1]
        if len(data) >= 3:
            test_method = data[2]
        _LOG.debug(
            "test=%s -> (%s, %s, %s)", test, file_name, test_class, test_method
        )
        if not os.path.exists(file_name):
            _LOG.warning("Can't find file '%s'", file_name)
        if target_type == "tests":
            targets.append(test)
        elif target_type == "files":
            if file_name != "":
                targets.append(file_name)
            else:
                _LOG.warning(
                    "Skipping test='%s' since file_name='%s'", test, file_name
                )
        elif target_type == "classes":
            if file_name != "" and test_class != "":
                targets.append(f"{file_name}::{test_class}")
            else:
                _LOG.warning(
                    "Skipping test='%s' since file_name='%s', test_class='%s'",
                    test,
                    file_name,
                    test_class,
                )
        else:
            hdbg.dfatal(f"Invalid target_type='{target_type}'")
    # Package the output.
    _LOG.debug("res=%s", str(targets))
    targets = hlist.remove_duplicates(targets)
    _LOG.info(
        "Found %d failed pytest '%s' targets:\n%s",
        len(targets),
        target_type,
        "\n".join(targets),
    )
    hdbg.dassert_isinstance(targets, list)
    res = " ".join(targets)
    _LOG.debug("res=%s", str(res))
    #
    _to_pbcopy(res, pbcopy)
    return res


# #############################################################################
# Linter.
# #############################################################################


# TODO(gp): When running `python_execute` we could launch it inside a
# container.
@task
def check_python_files(  # type: ignore
    ctx,
    python_compile=True,
    python_execute=False,
    modified=False,
    branch=False,
    last_commit=False,
    all_=False,
    files="",
):
    """
    Compile and execute Python files checking for errors.

    The params have the same meaning as in `_get_files_to_process()`.
    """
    _report_task()
    _ = ctx
    # We allow to filter through the user specified `files`.
    mutually_exclusive = False
    remove_dirs = True
    file_list = _get_files_to_process(
        modified,
        branch,
        last_commit,
        all_,
        files,
        mutually_exclusive,
        remove_dirs,
    )
    _LOG.debug("Found %d files:\n%s", len(file_list), "\n".join(file_list))
    # Filter keeping only Python files.
    _LOG.debug("Filtering for Python files")
    exclude_paired_jupytext = True
    file_list = hio.keep_python_files(file_list, exclude_paired_jupytext)
    _LOG.debug("file_list=%s", "\n".join(file_list))
    _LOG.info("Need to process %d files", len(file_list))
    if not file_list:
        _LOG.warning("No files were selected")
    # Scan all the files.
    failed_filenames = []
    for file_name in file_list:
        _LOG.info("Processing '%s'", file_name)
        if python_compile:
            import compileall

            success = compileall.compile_file(file_name, force=True, quiet=1)
            _LOG.debug("file_name='%s' -> python_compile=%s", file_name, success)
            if not success:
                msg = "'%s' doesn't compile correctly" % file_name
                _LOG.error(msg)
                failed_filenames.append(file_name)
        # TODO(gp): Add also `python -c "import ..."`, if not equivalent to `compileall`.
        if python_execute:
            cmd = f"python {file_name}"
            rc = hsysinte.system(cmd, abort_on_error=False, suppress_output=False)
            _LOG.debug("file_name='%s' -> python_compile=%s", file_name, rc)
            if rc != 0:
                msg = "'%s' doesn't execute correctly" % file_name
                _LOG.error(msg)
                failed_filenames.append(file_name)
    _LOG.info(
        "failed_filenames=%s\n%s",
        len(failed_filenames),
        "\n".join(failed_filenames),
    )
    error = len(failed_filenames) > 0
    return error


def _get_lint_docker_cmd(
    precommit_opts: str, run_bash: bool, stage: str, as_user: bool
) -> str:
    superproject_path, submodule_path = hgit.get_path_from_supermodule()
    if superproject_path:
        # We are running in a Git submodule.
        work_dir = f"/src/{submodule_path}"
        repo_root = superproject_path
    else:
        work_dir = "/src"
        repo_root = os.getcwd()
    _LOG.debug("work_dir=%s repo_root=%s", work_dir, repo_root)
    # TODO(gp): Do not hardwire the repo_short_name.
    # image = get_default_param("DEV_TOOLS_IMAGE_PROD")
    # image="*****.dkr.ecr.us-east-1.amazonaws.com/dev_tools:local"
    ecr_base_path = os.environ["AM_ECR_BASE_PATH"]
    image = f"{ecr_base_path}/dev_tools:{stage}"
    docker_cmd_ = ["docker run", "--rm"]
    if stage == "local":
        # For local stage also map repository root to /app.
        docker_cmd_.append(f"-v '{repo_root}':/app")
    if run_bash:
        docker_cmd_.append("-it")
    else:
        docker_cmd_.append("-t")
    if as_user:
        docker_cmd_.append(r"--user $(id -u):$(id -g)")
    docker_cmd_.extend(
        [
            f"-v '{repo_root}':/src",
            f"--workdir={work_dir}",
            f"{image}",
        ]
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
            hdbg.dassert_is_not(stage, None)
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
    stage="prod",
    as_user=True,
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
    :param stage: the image stage to use
    :param as_user: pass the user / group id or not
    """
    _report_task()
    lint_file_name = "linter_output.txt"
    # Remove the file.
    if os.path.exists(lint_file_name):
        cmd = f"rm {lint_file_name}"
        _run(ctx, cmd)
    #
    if only_format_steps:
        hdbg.dassert_eq(phases, "")
        phases = "isort black"
    if run_linter_step:
        # We don't want to run this all the times.
        # docker_pull(ctx, stage=stage, images="dev_tools")
        # Get the files to lint.
        # TODO(gp): For now we don't support linting the entire tree.
        all_ = False
        # For linting we can use only files modified in the client, in the branch, or
        # specified.
        mutually_exclusive = True
        # pre-commit doesn't handle directories, but only files.
        remove_dirs = True
        files_as_list = _get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files,
            mutually_exclusive,
            remove_dirs,
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
            cmd = _get_lint_docker_cmd(precommit_opts, run_bash, stage, as_user)
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
        branch_name = hgit.get_branch_name()
    elif branch == "master":
        branch_name = "master"
    elif branch == "all":
        branch_name = None
    else:
        raise ValueError("Invalid mode='%s'" % branch)
    # The output is tab separated. Parse it with csv and then filter.
    _, txt = hsysinte.system_to_string(cmd)
    _LOG.debug(hprint.to_str("txt"))
    # TODO(gp): This is a workaround for AmpTask1612.
    first_line = txt.split("\n")[0]
    num_cols = len(first_line.split("\t"))
    # STATUS            NAME        WORKFLOW  BRANCH        EVENT  ID   ELAPSED  AGE
    # Speculative fix   Slow tests  AmpTa...  pull_request  1097983981  1m1s     7m
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
    hdbg.dassert_in(num_cols, (8, 9))
    if num_cols == 9:
        cols.append("age")
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
        branch_name = hgit.get_branch_name()
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


def _get_repo_full_name_from_cmd(repo_short_name: str) -> Tuple[str, str]:
    """
    Convert the `repo_short_name` from command line (e.g., "current", "amp",
    "lm") to the repo_short_name full name without host name.
    """
    repo_full_name_with_host: str
    if repo_short_name == "current":
        # Get the repo name from the current repo.
        repo_full_name_with_host = hgit.get_repo_full_name_from_dirname(
            ".", include_host_name=True
        )
        # Compute the short repo name corresponding to "current".
        repo_full_name = hgit.get_repo_full_name_from_dirname(
            ".", include_host_name=False
        )
        ret_repo_short_name = hgit.get_repo_name(
            repo_full_name, in_mode="full_name", include_host_name=False
        )

    else:
        # Get the repo name using the short -> full name mapping.
        repo_full_name_with_host = hgit.get_repo_name(
            repo_short_name, in_mode="short_name", include_host_name=True
        )
        ret_repo_short_name = repo_short_name
    _LOG.debug(
        "repo_short_name=%s -> repo_full_name_with_host=%s ret_repo_short_name=%s",
        repo_short_name,
        repo_full_name_with_host,
        ret_repo_short_name,
    )
    return repo_full_name_with_host, ret_repo_short_name


def _get_gh_issue_title(issue_id: int, repo_short_name: str) -> Tuple[str, str]:
    """
    Get the title of a GitHub issue.

    :param repo_short_name: `current` refer to the repo_short_name where we are, otherwise
        a repo_short_name short name (e.g., "amp")
    """
    repo_full_name_with_host, repo_short_name = _get_repo_full_name_from_cmd(
        repo_short_name
    )
    # > (export NO_COLOR=1; gh issue view 1251 --json title)
    # {"title":"Update GH actions for amp"}
    hdbg.dassert_lte(1, issue_id)
    cmd = f"gh issue view {issue_id} --repo {repo_full_name_with_host} --json title,url"
    _, txt = hsysinte.system_to_string(cmd)
    _LOG.debug("txt=\n%s", txt)
    # Parse json.
    dict_ = json.loads(txt)
    _LOG.debug("dict_=\n%s", dict_)
    title = dict_["title"]
    _LOG.debug("title=%s", title)
    url = dict_["url"]
    _LOG.debug("url=%s", url)
    # Remove some annoying chars.
    for char in ": + ( ) / ` *".split():
        title = title.replace(char, "")
    # Replace multiple spaces with one.
    title = re.sub(r"\s+", " ", title)
    #
    title = title.replace(" ", "_")
    title = title.replace("-", "_")
    # Add the prefix `AmpTaskXYZ_...`
    task_prefix = hgit.get_task_prefix_from_repo_short_name(repo_short_name)
    _LOG.debug("task_prefix=%s", task_prefix)
    title = "%s%d_%s" % (task_prefix, issue_id, title)
    return title, url


@task
def gh_issue_title(ctx, issue_id, repo_short_name="current", pbcopy=True):  # type: ignore
    """
    Print the title that corresponds to the given issue and repo_short_name.
    E.g., AmpTask1251_Update_GH_actions_for_amp.

    :param pbcopy: save the result into the system clipboard (only on macOS)
    """
    _report_task(hprint.to_str("issue_id repo_short_name"))
    _ = ctx
    issue_id = int(issue_id)
    hdbg.dassert_lte(1, issue_id)
    title, url = _get_gh_issue_title(issue_id, repo_short_name)
    # Print or copy to clipboard.
    msg = f"{title}: {url}"
    _to_pbcopy(msg, pbcopy)


# TODO(gp): Add unit test for
# i gh_create_pr --no-draft --body="Misc changes while adding unit tests"


@task
def gh_create_pr(  # type: ignore
    ctx, body="", draft=True, repo_short_name="current", title=""
):
    """
    Create a draft PR for the current branch in the corresponding
    repo_short_name.

    :param body: the body of the PR
    :param draft: draft or ready-to-review PR
    """
    _report_task()
    branch_name = hgit.get_branch_name()
    if not title:
        # Use the branch name as title.
        title = branch_name
    repo_full_name_with_host, repo_short_name = _get_repo_full_name_from_cmd(
        repo_short_name
    )
    _LOG.info(
        "Creating PR with title '%s' for '%s' in %s",
        title,
        branch_name,
        repo_full_name_with_host,
    )
    # TODO(gp): Check whether the PR already exists.
    # TODO(gp): Use _to_single_line_cmd
    cmd = (
        "gh pr create"
        + f" --repo {repo_full_name_with_host}"
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
    hsysinte.system(cmd)
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
    Find all the files under `dir_name` that are owned or not by `user`.
    """
    _LOG.debug("")
    mode = "\\!" if not is_equal else ""
    cmd = f'find {dir_name} -name "*" {mode} -user "{user}"'
    _, txt = hsysinte.system_to_string(cmd)
    files: List[str] = txt.split("\n")
    return files


def _find_files_for_group(dir_name: str, group: str, is_equal: bool) -> List[str]:
    """
    Find all the files under `dir_name` that are owned by a group `group`.
    """
    _LOG.debug("")
    mode = "\\!" if not is_equal else ""
    cmd = f'find {dir_name} -name "*" {mode} -group "{group}"'
    _, txt = hsysinte.system_to_string(cmd)
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
    _, txt = hsysinte.system_to_string(cmd)
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
        txt1 += "%s(%s), " % (user, len(files))
    _LOG.info("user=%s", txt1)
    #
    txt2 = ""
    for group, files in group_to_files.items():
        txt2 += "%s(%s), " % (group, len(files))
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
        cmd = "ls -ld %s" % " ".join(files_tmp)
        _, txt_tmp = hsysinte.system_to_string(cmd)
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
        cmd = "%s %s" % (cmd, " ".join(files_tmp))
        hsysinte.system(cmd, abort_on_error=abort_on_error)


def _print_problems(dir_name: str = ".") -> None:
    """
    Do `ls -l` on files that are not owned by the current user and its group.

    This function is used for debugging.
    """
    _, _, file_to_user_group = _compute_stats_by_user_and_group(dir_name)
    user = hsysinte.get_user_name()
    docker_user = get_default_param("DOCKER_USER")
    # user_group = f"{user}_g"
    # shared_group = get_default_param("SHARED_GROUP")
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
    hsysinte.system(cmd, abort_on_error=abort_on_error)
    #
    cmd = f"cp {tmp_file} {file}"
    hsysinte.system(cmd, abort_on_error=abort_on_error)
    #
    cmd = f"rm -rf {tmp_file}"
    hsysinte.system(cmd, abort_on_error=abort_on_error)


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
    user = hsysinte.get_user_name()
    docker_user = get_default_param("DOCKER_USER")
    for file, (curr_user, _) in tqdm.tqdm(file_to_user_group.items()):
        if curr_user not in (user, docker_user):
            _LOG.info("Fixing file '%s'", file)
            hdbg.dassert_file_exists(file)
            cmd = f"ls -l {file}"
            hsysinte.system(
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
        user = hsysinte.get_user_name()
        user_group = f"{user}_g"
        shared_group = get_default_param("SHARED_GROUP")
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
            hsysinte.system(cmd, abort_on_error=abort_on_error)
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
    user = hsysinte.get_user_name()
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
            hsysinte.system(cmd, abort_on_error=abort_on_error)
        is_dir = os.path.isdir(file)
        if is_dir:
            # pylint: disable=line-too-long
            # From https://www.gnu.org/software/coreutils/manual/html_node/Directory-Setuid-and-Setgid.html
            # If a directory’s set-group-ID bit is set, newly created subfiles
            # inherit the same group as the directory,
            # pylint: enable=line-too-long
            has_set_group_id = st_mode & stat.S_ISGID
            if not has_set_group_id:
                cmd = f"chmod g+s {file}"
                if curr_user != user:
                    # For files not owned by the current user, we need to `sudo`.
                    cmd = f"sudo -u {curr_user} {cmd}"
                hsysinte.system(cmd, abort_on_error=abort_on_error)


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
