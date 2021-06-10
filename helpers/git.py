"""
Import as:

import helpers.git as git
"""

import collections
import functools
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)

# We refer to "Git" when we talk about the control system (e.g., "in a Git
# repository") and `git` when we refer to implementation of Git as a program
# installed in a computer.

# TODO(gp): Check
#  https://git-scm.com/book/en/v2/Appendix-B%3A-Embedding-Git-in-your-Applications-Dulwich

# TODO(gp): Avoid "stuttering": the module is already called "git", so no need
#  to make reference to git again.

# TODO(gp): Add mem caching to some functions below. We assume that one doesn't
#  change dir (which is a horrible idea) and thus we can memoize.

# TODO(gp): Spell super_module and sub_module always in the same way in both
#  comments and code. For simplicity (e.g., instead of `super_module` in code and
#  `super-module` in comment) we might want to spell `supermodule` everywhere.

# #############################################################################
# Git submodule functions
# #############################################################################


@functools.lru_cache()
def get_client_root(super_module: bool) -> str:
    """
    Return the full path of the root of the Git client.

    E.g., `/Users/saggese/src/.../amp`.

    :param super_module: if True use the root of the Git super_module,
        if we are in a submodule. Otherwise use the Git sub_module root
    """
    if super_module and is_inside_submodule():
        # https://stackoverflow.com/questions/957928
        # > cd /Users/saggese/src/.../amp
        # > git rev-parse --show-superproject-working-tree
        # /Users/saggese/src/...
        cmd = "git rev-parse --show-superproject-working-tree"
    else:
        # > git rev-parse --show-toplevel
        # /Users/saggese/src/.../amp
        cmd = "git rev-parse --show-toplevel"
    # TODO(gp): Use system_to_one_line().
    _, out = hsinte.system_to_string(cmd)
    out = out.rstrip("\n")
    dbg.dassert_eq(len(out.split("\n")), 1, msg="Invalid out='%s'" % out)
    client_root: str = os.path.realpath(out)
    return client_root


@functools.lru_cache()
def get_branch_name(dir_name: str = ".") -> str:
    """
    Return the name of the Git branch including a certain dir.

    E.g., `master` or `AmpTask672_Add_script_to_check_and_merge_PR`
    """
    dbg.dassert_exists(dir_name)
    # > git rev-parse --abbrev-ref HEAD
    # master
    cmd = "cd %s && git rev-parse --abbrev-ref HEAD" % dir_name
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, output = data
    return output


@functools.lru_cache()
def is_inside_submodule(git_dir: str = ".") -> bool:
    """
    Return whether a dir is inside a Git submodule or a Git supermodule.

    We determine this checking if the current git repo is included
    inside another git repo.
    """
    cmd = []
    # - Find the git root of the current directory
    # - Check if the dir one level up is a valid git repo
    # Go to the dir.
    cmd.append("cd %s" % git_dir)
    # > cd im/
    # > git rev-parse --show-toplevel
    # /Users/saggese/src/.../amp
    cmd.append('cd "$(git rev-parse --show-toplevel)/.."')
    # > git rev-parse --is-inside-work-tree
    # true
    cmd.append("(git rev-parse --is-inside-work-tree | grep -q true)")
    cmd_as_str = " && ".join(cmd)
    rc = hsinte.system(cmd_as_str, abort_on_error=False)
    ret: bool = rc == 0
    return ret


def _is_repo(repo_short_name: str) -> bool:
    """
    Return whether we are inside `amp` and `amp` is a submodule.
    """
    repo_full_name = get_repo_full_name_from_dirname(".")
    return get_repo_short_name(repo_full_name) == repo_short_name


def is_amp() -> bool:
    """
    Return whether we are inside `amp` and `amp` is a sub-module.
    """
    return _is_repo("amp")


def is_lem() -> bool:
    """
    Return whether we are inside `lem` and `lem` is a sub-module.
    """
    return _is_repo("lem")


# TODO(gp): submodule -> sub_module
def is_in_amp_as_submodule() -> bool:
    """
    Return whether we are in the `amp` repo and it's a sub-module, e.g., of
    `lem`.
    """
    return is_amp() and is_inside_submodule(".")


# TODO(gp): supermodule -> super_module
def is_in_amp_as_supermodule() -> bool:
    """
    Return whether we are in the `amp` repo and it's a super-module, i.e.,
    `amp` by itself.
    """
    return is_amp() and not is_inside_submodule(".")


# #############################################################################


def _get_submodule_hash(dir_name: str) -> str:
    """
    Report the Git hash that a submodule (e.g., amp) is at from the point of
    view of a supermodule.

    > git ls-tree master | grep <dir_name>
    """
    dbg.dassert_exists(dir_name)
    cmd = "git ls-tree master | grep %s" % dir_name
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, output = data
    # 160000 commit 0011776388b4c0582161eb2749b665fc45b87e7e  amp
    _LOG.debug("output=%s", output)
    data: List[str] = output.split()
    _LOG.debug("data=%s", data)
    git_hash = data[2]
    return git_hash


@functools.lru_cache()
def get_path_from_supermodule() -> Tuple[str, str]:
    """
    Return the path to the Git repo including the Git submodule for a
    submodule, and return empty for a supermodule. See AmpTask1017.

    E.g.,
    - for amp included in another repo returns 'amp'
    - for amp without supermodule returns ''
    """
    cmd = "git rev-parse --show-superproject-working-tree"
    # > cd /Users/saggese/src/.../lem/amp
    # > git rev-parse --show-superproject-working-tree
    # /Users/saggese/src/.../lem
    #
    # > cd /Users/saggese/src/.../lem
    # > git rev-parse --show-superproject-working-tree
    # (No result)
    superproject_path: str = hsinte.system_to_one_line(cmd)[1]
    _LOG.debug("superproject_path='%s'", superproject_path)
    #
    cmd = (
        f"git config --file {superproject_path}/.gitmodules --get-regexp path"
        '| grep $(basename "$(pwd)")'
        "| awk '{ print $2 }'"
    )
    # > git config --file /Users/saggese/src/.../.gitmodules --get-regexp path
    # submodule.amp.path amp
    submodule_path: str = hsinte.system_to_one_line(cmd)[1]
    _LOG.debug("submodule_path='%s'", submodule_path)
    return superproject_path, submodule_path


@functools.lru_cache()
def get_submodule_paths() -> List[str]:
    """
    Return the path of the submodules in this repo, e.g., `["amp"]` or `[]`.
    """
    # > git config --file .gitmodules --get-regexp path
    # submodule.amp.path amp
    cmd = "git config --file .gitmodules --get-regexp path | awk '{ print $2 }'"
    _, txt = hsinte.system_to_string(cmd)
    _LOG.debug("txt=%s", txt)
    files: List[str] = hsinte.text_to_list(txt)
    _LOG.debug("files=%s", files)
    return files


def has_submodules() -> bool:
    return len(get_submodule_paths()) > 0


# #############################################################################


def _get_hash(git_hash: str, short_hash: bool, num_digits: int = 8) -> str:
    dbg.dassert_lte(1, num_digits)
    if short_hash:
        ret = git_hash[:num_digits]
    else:
        ret = git_hash
    return ret


def _group_hashes(head_hash: str, remh_hash: str, subm_hash: str) -> str:
    """
    head_hash: a
    remh_hash: b
    subm_hash: c
    """
    map_ = collections.OrderedDict()
    map_["head_hash"] = head_hash
    map_["remh_hash"] = remh_hash
    if subm_hash:
        map_["subm_hash"] = subm_hash
    #
    inv_map = collections.OrderedDict()
    for k, v in map_.items():
        if v not in inv_map:
            inv_map[v] = [k]
        else:
            inv_map[v].append(k)
    #
    txt = []
    for k, v in inv_map.items():
        # Transform:
        #   ('a2bfc704', ['head_hash', 'remh_hash'])
        # into
        #   'head_hash = remh_hash = a2bfc704'
        txt.append("%s = %s" % (" = ".join(v), k))
    txt = "\n".join(txt)
    return txt


def report_submodule_status(dir_names: List[str], short_hash: bool) -> str:
    """
    Return a string representing the status of the repos in `dir_names`.
    """
    txt = []
    for dir_name in dir_names:
        txt.append("dir_name='%s'" % dir_name)
        txt.append("  is_inside_submodule: %s" % is_inside_submodule(dir_name))
        #
        branch_name = get_branch_name(dir_name)
        if branch_name != "master":
            branch_name = "!!! %s !!!" % branch_name
        txt.append("  branch: %s" % branch_name)
        #
        head_hash = get_head_hash(dir_name)
        head_hash = _get_hash(head_hash, short_hash)
        txt.append("  head_hash: %s" % head_hash)
        #
        remh_hash = get_remote_head_hash(dir_name)
        remh_hash = _get_hash(remh_hash, short_hash)
        txt.append("  remh_hash: %s" % remh_hash)
        #
        if dir_name != ".":
            subm_hash = _get_submodule_hash(dir_name)
            subm_hash = _get_hash(subm_hash, short_hash)
            txt.append("  subm_hash: %s" % subm_hash)
    txt_as_str = "\n".join(txt)
    return txt_as_str


# #############################################################################
# GitHub repository name
# #############################################################################


# All functions should take as input `repo_short_name` and have a switch `mode`
# to distinguish full vs short repo name.


def _parse_github_repo_name(repo_name: str) -> str:
    """
    Parse a repo name from `git remote` in the format:
    `git@github.com:alphamatic/amp` or `https://github.com/alphamatic/amp`

    and return "alphamatic/amp"
    """
    m = re.match(r"^\S+\.com[:/](.*)$", repo_name)
    dbg.dassert(m, "Can't parse '%s'", repo_name)
    repo_name = m.group(1)  # type: ignore
    _LOG.debug("repo_name=%s", repo_name)
    # We expect something like "alphamatic/amp".
    m = re.match(r"^\S+/\S+$", repo_name)
    dbg.dassert(m, "repo_name='%s'", repo_name)
    # origin  git@github.com:.../ORG_....git (fetch)
    suffix_to_remove = ".git"
    if repo_name.endswith(suffix_to_remove):
        repo_name = repo_name[: -len(suffix_to_remove)]
    return repo_name


def get_repo_full_name_from_dirname(dir_name: str) -> str:
    """
    :return: the full name of the repo in `git_dir`, e.g., "alphamatic/amp".
    """
    dbg.dassert_exists(dir_name)
    #
    cmd = "cd %s; (git remote -v | grep origin | grep fetch)" % dir_name
    _, output = hsinte.system_to_string(cmd)
    # > git remote -v
    # origin  git@github.com:alphamatic/amp (fetch)
    # origin  git@github.com:alphamatic/amp (push)
    # TODO(gp): Make it more robust, by checking both fetch and push.
    #  "origin  git@github.com:alphamatic/amp (fetch)"
    data: List[str] = output.split()
    _LOG.debug("data=%s", data)
    dbg.dassert_eq(len(data), 3, "data='%s'", str(data))
    # Extract the middle string, e.g., "git@github.com:alphamatic/amp"
    repo_name = data[1]
    #
    repo_name = _parse_github_repo_name(repo_name)
    return repo_name


def get_repo_full_name_from_client(super_module: bool) -> str:
    """
    Return the full name of the repo (e.g., "alphamatic/amp") from a Git
    client.

    :param super_module: like in get_client_root()
    """
    # Get the git remote in the git_module.
    git_dir = get_client_root(super_module)
    repo_name = get_repo_full_name_from_dirname(git_dir)
    return repo_name


@functools.lru_cache()
def _get_repo_short_to_full_name() -> Dict[str, str]:
    """
    Return the map from short name (e.g., "amp") to full name
    ("alphamatic/amp").
    """
    repo_map = {
        "amp": "alphamatic/amp",
        #"lem": "alphamatic/lemonade",
        "dev_tools": "alphamatic/dev_tools",
    }
    # TODO(gp): We should actually ask Git where the supermodule is.
    file_name = "./repo_config.py"
    dbg.dassert_file_exists(file_name)
    txt = hio.from_file(file_name)
    exec(txt, globals())
    repo_map.update(get_repo_map())
    dbg.dassert_no_duplicates(repo_map.keys())
    dbg.dassert_no_duplicates(repo_map.values())
    return repo_map


@functools.lru_cache()
def _get_repo_full_to_short_name() -> Dict[str, str]:
    """
    Return the map from full name ("alphamatic/amp") to short name (e.g.,
    "amp").
    """
    # Get the reverse map.
    repo_map = _get_repo_short_to_full_name()
    inv_repo_map = {v: k for (k, v) in repo_map.items()}
    return inv_repo_map


# TODO(gp): Use only `get_repo_name(..., mode)`.
def get_repo_full_name(short_name: str) -> str:
    """
    Return the full name of a git repo based on its short name.

    E.g., "amp" -> "alphamatic/amp"
    """
    repo_map = _get_repo_short_to_full_name()
    dbg.dassert_in(short_name, repo_map, "Invalid short_name='%s'", short_name)
    return repo_map[short_name]


def get_repo_short_name(full_name: str) -> str:
    """
    Return the short name of a git repo based on its full name.

    E.g., "alphamatic/amp" -> "amp"
    """
    repo_map = _get_repo_full_to_short_name()
    dbg.dassert_in(full_name, repo_map, "Invalid full_name='%s'", full_name)
    return repo_map[full_name]


def get_repo_name(name: str, in_mode: str) -> str:
    """
    Return the full / short name of a git repo based on the alternative name.

    :param in_mode: the values `full_name` or `short_name` determine how to interpret
        `name`
    """
    if in_mode == "full_name":
        ret = get_repo_short_name(name)
    elif in_mode == "short_name":
        ret = get_repo_full_name(name)
    else:
        raise ValueError("Invalid mode='%s'" % in_mode)
    return ret


def get_all_repo_names(mode: str) -> List[str]:
    """
    Return all the repo full or short names.

    :param mode: if "full_name" return the full names (e.g., "alphamatic/amp")
        if "short_name" return the short names (e.g., "amp")
    """
    if mode == "full_name":
        repo_map = _get_repo_short_to_full_name()
    elif mode == "short_name":
        repo_map = _get_repo_full_to_short_name()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    return sorted(list(repo_map.values()))


def get_task_prefix_from_repo_short_name(repo_short_name: str) -> str:
    """
    Return the task prefix for a repo (e.g., "amp" -> "AmpTask").
    """
    # TODO(gp): Find a better way rather than centralizing the names of all repos.
    if repo_short_name == "amp":
        prefix = "AmpTask"
    elif repo_short_name == "lem":
        prefix = "LemTask"
    elif repo_short_name == "dev_tools":
        prefix = "DevToolsTask"
    else:
        raise ValueError("Invalid short_name='%s'" % repo_short_name)
    return prefix


# #############################################################################
# Git path
# #############################################################################


@functools.lru_cache()
def find_file_in_git_tree(file_name: str, super_module: bool = True) -> str:
    """
    Find the path of a file in a Git tree.

    We get the Git root and then search for the file from there.
    """
    root_dir = get_client_root(super_module=super_module)
    # TODO(gp): Use -not -path '*/\.git/*'
    cmd = "find %s -name '%s' | grep -v .git" % (root_dir, file_name)
    _, file_name = hsinte.system_to_one_line(cmd)
    _LOG.debug("file_name=%s", file_name)
    dbg.dassert_ne(
        file_name, "", "Can't find file '%s' in dir '%s'", file_name, root_dir
    )
    file_name: str = os.path.abspath(file_name)
    dbg.dassert_exists(file_name)
    return file_name


def get_path_from_git_root(file_name: str, super_module: bool) -> str:
    """
    Get the git path from the root of the tree.

    :param super_module: like get_client_root()
    """
    # Get Git root.
    git_root = get_client_root(super_module) + "/"
    # TODO(gp): Use os.path.relpath()
    # abs_path = os.path.abspath(file_name)
    # dbg.dassert(abs_path.startswith(git_root))
    # end_idx = len(git_root)
    # ret = abs_path[end_idx:]
    ret = os.path.relpath(file_name, git_root)
    return ret


@functools.lru_cache()
def get_amp_abs_path() -> str:
    """
    Return the absolute path of `amp` dir.
    """
    repo_sym_name = get_repo_full_name_from_client(super_module=False)
    if repo_sym_name == "alphamatic/amp":
        # If we are in the amp repo, then the git client root is the amp
        # directory.
        git_root = get_client_root(super_module=False)
        amp_dir = git_root
    else:
        # If we are not in the amp repo, then look for the amp dir.
        amp_dir = find_file_in_git_tree("amp", super_module=True)
        git_root = get_client_root(super_module=True)
        amp_dir = os.path.join(git_root, amp_dir)
    amp_dir = os.path.abspath(amp_dir)
    # Sanity check.
    dbg.dassert_dir_exists(amp_dir)
    return amp_dir


# TODO(gp): Is this needed?
def get_repo_dirs() -> List[str]:
    """
    Return the list of the repo repositories, e.g., `[".", "amp", "infra"]`.
    """
    dir_names = ["."]
    dirs = ["amp"]
    for dir_name in dirs:
        if os.path.exists(dir_name):
            dir_names.append(dir_name)
    return dir_names


def purify_docker_file_from_git_client(
    file_name: str,
    super_module: Optional[bool],
    dir_depth: int = 1,
    mode: str = "return_all_results",
) -> Tuple[bool, str]:
    """
    Convert a file or dir that was generated inside Docker to a file in the
    current Git client.

    This operation is best effort since it might not be able to find the
    corresponding file in the current repo.

    E.g.,
    - A file like '/app/amp/core/dataflow_model/utils.py', in a Docker container with
      Git root in '/app' becomes 'amp/core/dataflow_model/utils.py'
    - For a file like '/app/amp/core/dataflow_model/utils.py' outside Docker, we look
      for the file 'dataflow_model/utils.py' in the current client and then normalize
      with respect to the

    :param super_module:
        - True/False: the file is with respect to a Git repo
        - `None`: the file is returned as relative to current dir
    :param dir_depth: same meaning as in `find_file_with_dir()`
    :param mode: same as `system_interaction.select_result_file_from_list()`
    :return: the best guess for the file name corresponding to `file_name`
    """
    _LOG.debug("# Processing file_name='%s'", file_name)
    dbg.dassert_isinstance(file_name, str)
    # Clean up file name.
    file_name = os.path.normpath(file_name)
    _LOG.debug("file_name=%s", file_name)
    file_names = hsinte.find_file_with_dir(
        file_name, ".", dir_depth=dir_depth, mode=mode
    )
    _LOG.debug("file_names=%s", file_names)
    if len(file_names) == 0:
        # We didn't find the file in the current client: leave the file as it was.
        _LOG.warning("Can't find file corresponding to '%s'", file_name)
        found = False
    elif len(file_names) == 1:
        # We have found the file.
        file_name = file_names[0]
        _LOG.debug("file_name=%s", file_name)
        #
        if super_module is not None:
            file_name = get_path_from_git_root(file_name, super_module)
        file_name = os.path.normpath(file_name)
        found = True
    else:
        _LOG.warning(
            "Found multiple potential files corresponding to '%s'", file_name
        )
        file_name = ",".join(file_names)
        found = False
    _LOG.debug("-> found=%s file_name='%s'", found, file_name)
    return (found, file_name)


# #############################################################################
# Git hash
# #############################################################################


def get_head_hash(dir_name: str = ".", short_hash: bool = False) -> str:
    """
    Report the hash that a Git repo is synced at.

    ```
    > git rev-parse HEAD
    4759b3685f903e6c669096e960b248ec31c63b69
    ```
    """
    dbg.dassert_exists(dir_name)
    opts = "--short " if short_hash else " "
    cmd = f"cd {dir_name} && git rev-parse {opts}HEAD"
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, output = data
    return output


# TODO(gp): Use get_head_hash() and remove this.
def get_current_commit_hash(dir_name: str = ".") -> str:
    dbg.dassert_exists(dir_name)
    cmd = f"cd {dir_name} && git rev-parse HEAD"
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, sha = data
    # 0011776388b4c0582161eb2749b665fc45b87e7e
    _LOG.debug("sha=%s", sha)
    return sha


def get_remote_head_hash(dir_name: str) -> str:
    """
    Report the hash that the remote Git repo is at.
    """
    dbg.dassert_exists(dir_name)
    sym_name = get_repo_full_name_from_dirname(dir_name)
    cmd = f"git ls-remote git@github.com:{sym_name} HEAD 2>/dev/null"
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, output = data
    # > git ls-remote git@github.com:alphamatic/amp HEAD 2>/dev/null
    # 921676624f6a5f3f36ab507baed1b886227ac2e6        HEAD
    return output


# #############################################################################
# Modified files
# #############################################################################


def get_modified_files(
    dir_name: str = ".", remove_files_non_present: bool = True
) -> List[str]:
    """
    Return the files that are added and modified in the Git client.

    In other words the files that will be committed with a `git commit -am ...`.
    Equivalent to `dev_scripts/git_files.sh`

    :param dir_name: directory with Git client
    :param remove_files_non_present: remove the files that are not
        currently present in the client
    :return: list of files
    """
    # If the client status is:
    #   > git status -s
    #   AM dev_scripts/infra/ssh_tunnels.py
    #   M helpers/git.py
    #   ?? linter_warnings.txt
    #
    # The result is:
    #   > git diff --cached --name-only
    #   dev_scripts/infra/ssh_tunnels.py
    #
    #   > git ls-files -m
    #   dev_scripts/infra/ssh_tunnels.py
    #   helpers/git.py
    cmd = "(git diff --cached --name-only; git ls-files -m) | sort | uniq"
    files: List[str] = hsinte.system_to_files(
        cmd, dir_name, remove_files_non_present
    )
    return files


# TODO(gp): -> ...previously...
def get_previous_committed_files(
    dir_name: str = ".",
    num_commits: int = 1,
    remove_files_non_present: bool = True,
) -> List[str]:
    """
    Return files changed in the Git client in the last `num_commits` commits.

    Equivalent to `dev_scripts/git_previous_commit_files.sh`

    :param dir_name: directory with Git client
    :param num_commits: how many commits in the past to consider
    :param remove_files_non_present: remove the files that are not currently present
        in the client
    :return: list of files
    """
    cmd = []
    cmd.append('git show --pretty="" --name-only')
    cmd.append("$(git log --author $(git config user.name) -%d" % num_commits)
    cmd.append(r"""| \grep "^commit " | perl -pe 's/commit (.*)/$1/')""")
    cmd_as_str = " ".join(cmd)
    files: List[str] = hsinte.system_to_files(
        cmd_as_str, dir_name, remove_files_non_present
    )
    return files


def get_modified_files_in_branch(
    dst_branch: str, dir_name: str = ".", remove_files_non_present: bool = True
) -> List[str]:
    """
    Return files modified in the current branch with respect to `dst_branch`.

    Equivalent to `git diff --name-only master...`
    Please remember that there is a difference between `master` and `origin/master`.
    See https://stackoverflow.com/questions/18137175

    :param dir_name: directory with Git client
    :param dst_branch: branch to compare to, e.g., master
    :param remove_files_non_present: remove the files that are not
        currently present in the client
    :return: list of files
    """
    cmd = "git diff --name-only %s..." % dst_branch
    files: List[str] = hsinte.system_to_files(
        cmd, dir_name, remove_files_non_present
    )
    return files


def get_summary_files_in_branch(
    dst_branch: str,
    dir_name: str = ".",
) -> str:
    """
    Report summary of files in the current branch with respect to `dst_branch'.

    Same interface as `get_modified_files_in_branch`.
    """
    # File types (from https://git-scm.com/docs/git-diff).
    file_types = [
        ("added", "A"),
        ("copied", "C"),
        ("deleted", "D"),
        ("modified", "M"),
        ("renamed", "R"),
        ("type changed", "T"),
        ("unmerged", "U"),
        ("unknown", "X"),
        ("broken pairing", "B"),
    ]
    res = ""
    for tag, diff_type in file_types:
        cmd = f"git diff --diff-filter={diff_type} --name-only {dst_branch}..."
        files = hsinte.system_to_files(
            cmd, dir_name, remove_files_non_present=False
        )
        _LOG.debug("files=%s", "\n".join(files))
        if files:
            res += f"# {tag}: {len(files)}\n"
            res += hprint.indent("\n".join(files)) + "\n"
    res = res.rstrip("\n")
    return res


# #############################################################################
# Git commands.
# #############################################################################


# TODO(gp): -> get_user_name()
@functools.lru_cache()
def get_git_name() -> str:
    """
    Return the git user name.
    """
    cmd = "git config --get user.name"
    # For some reason data is annotated as Any by mypy, instead of
    # Tuple[int, str] so we need to cast it to the right value.
    data: Tuple[int, str] = hsinte.system_to_one_line(cmd)
    _, output = data
    return output


def git_log(num_commits: int = 5, my_commits: bool = False) -> str:
    """
    Return the output of a pimped version of git log.

    :param num_commits: number of commits to report
    :param my_commits: True to report only the current user commits
    :return: string
    """
    cmd = []
    cmd.append("git log --date=local --oneline --graph --date-order --decorate")
    cmd.append(
        "--pretty=format:" "'%h %<(8)%aN%  %<(65)%s (%>(14)%ar) %ad %<(10)%d'"
    )
    cmd.append("-%d" % num_commits)
    if my_commits:
        # This doesn't work in a container if the user relies on `~/.gitconfig` to
        # set the user name.
        # TODO(gp): We should use `get_git_name()`.
        cmd.append("--author $(git config user.name)")
    cmd = " ".join(cmd)
    data: Tuple[int, str] = hsinte.system_to_string(cmd)
    _, txt = data
    return txt


def git_stash_push(
    prefix: str, msg: Optional[str] = None, log_level: int = logging.DEBUG
) -> Tuple[str, bool]:
    import helpers.datetime_ as hdatet

    user_name = hsinte.get_user_name()
    server_name = hsinte.get_server_name()
    timestamp = hdatet.get_timestamp()
    tag = "%s-%s-%s" % (user_name, server_name, timestamp)
    tag = prefix + "." + tag
    _LOG.debug("tag='%s'", tag)
    cmd = "git stash push"
    _LOG.debug("msg='%s'", msg)
    push_msg = tag[:]
    if msg:
        push_msg += ": " + msg
    cmd += " -m '%s'" % push_msg
    hsinte.system(cmd, suppress_output=False, log_level=log_level)
    # Check if we actually stashed anything.
    cmd = r"git stash list | \grep '%s' | wc -l" % tag
    _, output = hsinte.system_to_string(cmd)
    was_stashed = int(output) > 0
    if not was_stashed:
        msg = "Nothing was stashed"
        _LOG.warning(msg)
        # raise RuntimeError(msg)
    return tag, was_stashed


def git_stash_apply(mode: str, log_level: int = logging.DEBUG) -> None:
    _LOG.debug("# Checking stash head ...")
    cmd = "git stash list | head -3"
    hsinte.system(cmd, suppress_output=False, log_level=log_level)
    #
    _LOG.debug("# Restoring local changes...")
    if mode == "pop":
        cmd = "git stash pop --quiet"
    elif mode == "apply":
        cmd = "git stash apply --quiet"
    else:
        raise ValueError("mode='%s'" % mode)
    hsinte.system(cmd, suppress_output=False, log_level=log_level)


def git_add_update(
    file_list: Optional[List[str]] = None, log_level: int = logging.DEBUG
) -> None:
    """
    Add list of files to git.

    If `file_list` is not specified, it adds all modified files.

    :param file_list: list of files to `git add`
    """
    _LOG.debug("# Adding all changed files to staging ...")
    cmd = "git add %s" % (" ".join(file_list) if file_list is not None else "-u")
    hsinte.system(cmd, suppress_output=False, log_level=log_level)


def fetch_origin_master_if_needed() -> None:
    """
    If inside CI system, force fetching `master` branch from Git repo.

    When testing a branch, `master` is not always fetched, but it might
    be needed by tests.
    """
    if hsinte.is_inside_ci():
        _LOG.warning("Running inside CI so fetching master")
        cmd = "git branch -a"
        _, txt = hsinte.system_to_string(cmd)
        _LOG.debug("%s=%s", cmd, txt)
        cmd = r'git branch -a | egrep "\s+master\s*$" | wc -l'
        # * (HEAD detached at pull/1337/merge)
        # master
        # remotes/origin/master
        # remotes/pull/1337/merge
        _, num = hsinte.system_to_one_line(cmd)
        num = int(num)
        _LOG.debug("num=%s", num)
        if num == 0:
            # See AmpTask1321 and AmpTask1338 for details.
            cmd = "git fetch origin master:refs/remotes/origin/master"
            hsinte.system(cmd)
            cmd = "git branch --track master origin/master"
            hsinte.system(cmd)
