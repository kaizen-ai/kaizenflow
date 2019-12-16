"""
Import as:

import helpers.git as git
"""

import logging
import os
import re
from typing import Dict, List

import helpers.datetime_ as hdt
import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# TODO(gp): Check https://git-scm.com/book/en/v2/Appendix-B%3A-Embedding-Git-in-your-Applications-Dulwich

# TODO(gp): Avoid "stuttering": the module is already called "git", so no need
#  to make reference to git again.


def _system_to_one_string(cmd) -> str:
    _, output = si.system_to_string(cmd)
    res = si.get_first_line(output)
    return res


# TODO(gp): -> get_user_name(). No stuttering.
def get_git_name() -> str:
    """
    Return the git user name.
    """
    cmd = "git config --get user.name"
    return _system_to_one_string(cmd)


def get_branch_name() -> str:
    """
    Return the name of the Git branch we are in.

    E.g., `master` or `PartTask672_DEV_INFRA_Add_script_to_check_and_merge_PR`
    """
    cmd = "git rev-parse --abbrev-ref HEAD"
    return _system_to_one_string(cmd)


# TODO(gp): Add mem caching to some functions below. We assume that one doesn't
#  change dir (which is a horrible idea) and thus we can memoize.
def is_inside_submodule() -> bool:
    """
    Return whether we are inside a Git submodule or in a Git supermodule.
    """
    cmd = (
        'cd "$(git rev-parse --show-toplevel)/.." && '
        "(git rev-parse --is-inside-work-tree | grep -q true)"
    )
    rc = si.system(cmd, abort_on_error=False)
    ret = rc == 0
    return ret


def get_client_root(super_module: bool) -> str:
    """
    Return the full path of the root of the Git client.
    E.g., "/Users/saggese/src/.../amp"

    :param super_module: if True use the root of the Git _super_module,
        if we are in a submodule.
        Otherwise use the Git _sub_module root
    """
    if super_module and is_inside_submodule():
        # https://stackoverflow.com/questions/957928
        cmd = "git rev-parse --show-superproject-working-tree"
    else:
        cmd = "git rev-parse --show-toplevel"
    _, out = si.system_to_string(cmd)
    out = out.rstrip("\n")
    dbg.dassert_eq(len(out.split("\n")), 1, msg="Invalid out='%s'" % out)
    client_root = os.path.realpath(out)
    return client_root


def find_file_in_git_tree(file_in: str, super_module: bool = True) -> str:
    """
    Find the path of a file `file_in` in the outermost git submodule (i.e.,
    in the super-module).
    """
    root_dir = get_client_root(super_module=super_module)
    cmd = "find %s -name '%s' | grep -v .git" % (root_dir, file_in)
    _, file_name = si.system_to_string(cmd)
    _LOG.debug("file_name=%s", file_name)
    # Make sure that there is a single outcome.
    dbg.dassert_eq(len(file_name.split("\n")), 1, "file_name=%s", file_name)
    dbg.dassert(
        file_name != "", "Can't find file '%s' in dir '%s'", file_in, root_dir
    )
    file_name = os.path.abspath(file_name)
    dbg.dassert_exists(file_name)
    return file_name


def get_repo_symbolic_name_from_dirname(git_dir: str) -> str:
    dbg.dassert_exists(git_dir)
    cmd = "cd %s; (git remote -v | grep fetch)" % git_dir
    # TODO(gp): Make it more robust, by checking both fetch and push.
    # "origin  git@github.com:alphamatic/amp (fetch)"
    _, output = si.system_to_string(cmd)
    data = output.split()
    _LOG.debug("data=%s", data)
    dbg.dassert_eq(len(data), 3, "data='%s'", str(data))
    # git@github.com:alphamatic/amp
    repo_name = data[1]
    m = re.match(r"^.*\.com:(.*)$", repo_name)
    dbg.dassert(m, "Can't parse '%s'", repo_name)
    repo_name = m.group(1)  # type: ignore
    _LOG.debug("repo_name=%s", repo_name)
    # We expect something like "alphamatic/amp".
    m = re.match(r"^\S+/\S+$", repo_name)
    dbg.dassert(m, "repo_name='%s'", repo_name)
    # origin  git@github.com:ParticleDev/ORG_Particle.git (fetch)
    suffix_to_remove = ".git"
    if repo_name.endswith(suffix_to_remove):
        repo_name = repo_name[: -len(suffix_to_remove)]
    return repo_name


def get_repo_symbolic_name(super_module: bool) -> str:
    """
    Return the name of the remote repo.
    E.g., "alphamatic/amp", "ParticleDev/commodity_research"

    :param super_module: like get_client_root()
    """
    # Get the git remote in the git_module.
    git_dir = get_client_root(super_module)
    repo_name = get_repo_symbolic_name_from_dirname(git_dir)
    return repo_name


def _get_repo_map() -> Dict[str, str]:
    repo_map = {"alphamatic/amp": "Amp"}
    # TODO(gp): The proper fix is #PartTask551.
    # Get info from the including repo, if possible.
    try:
        import repo_config as repc  # type: ignore

        repo_map.update(repc.REPO_MAP)
    except ImportError:
        _LOG.debug("No including repo")
    dbg.dassert_no_duplicates(repo_map.keys())
    dbg.dassert_no_duplicates(repo_map.values())
    return repo_map.copy()  # type: ignore


def get_all_repo_symbolic_names() -> List[str]:
    repo_map = _get_repo_map()
    return repo_map.values()  # type: ignore


# TODO(gp): Found a better name.
def get_repo_prefix(repo_github_name) -> str:
    """
    Return the symbolic name of a git repo.
    E.g., for "alphamatic/amp", the function returns "Amp".
    """
    repo_map = _get_repo_map()
    dbg.dassert_in(repo_github_name, repo_map, "Invalid repo github name")
    return repo_map[repo_github_name]


def get_repo_github_name(repo_symbolic_name: str) -> str:
    # Get the reverse map.
    repo_map = _get_repo_map()
    inv_repo_map = {v: k for (k, v) in repo_map.items()}
    #
    dbg.dassert_in(repo_symbolic_name, inv_repo_map, "Invalid repo symbolic name")
    return inv_repo_map[repo_symbolic_name]


def get_path_from_git_root(file_name: str, super_module: bool) -> str:
    """
    Get the git path from the root of the tree.

    :param super_module: like get_client_root()
    """
    git_root = get_client_root(super_module) + "/"
    abs_path = os.path.abspath(file_name)
    dbg.dassert(abs_path.startswith(git_root))
    end_idx = len(git_root)
    ret = abs_path[end_idx:]
    # cmd = "git ls-tree --full-name --name-only HEAD %s" % file_name
    # _, git_file_name = si.system_to_string(cmd)
    # dbg.dassert_ne(git_file_name, "")
    return ret


def get_amp_abs_path() -> str:
    """
    Return the absolute path of `amp` dir.
    """
    repo_sym_name = get_repo_symbolic_name(super_module=False)
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
    if si.get_user_name() != "jenkins":
        # Jenkins checks out amp repo in directories with different names,
        # e.g., amp.dev.build_clean_env.run_slow_coverage_tests.
        dbg.dassert_eq(os.path.basename(amp_dir), "amp")
    return amp_dir


def get_submodule_hash(dir_name: str) -> str:
    """
    Report the Git hash that a submodule (e.g., amp) is at from the point of
    view of a supermodule (e.g., p1).

    > git ls-tree master | grep <dir_name>
    """
    dbg.dassert_exists(dir_name)
    cmd = "git ls-tree master | grep %s" % dir_name
    _, output = si.system_to_one_line_string(cmd)
    # 160000 commit 0011776388b4c0582161eb2749b665fc45b87e7e  amp
    data = output.split(" ")
    git_hash = data[2]
    return git_hash


def get_hash_head(dir_name: str) -> str:
    """
    Report the hash that a Git repo is synced at.

    > git rev-parse HEAD
    """
    dbg.dassert_exists(dir_name)
    cmd = "git rev-parse HEAD"
    _, output = si.system_to_one_line_string(cmd)
    # 4759b3685f903e6c669096e960b248ec31c63b69
    return output


# #############################################################################


def _check_files(files: List[str]) -> List[str]:
    files_tmp = []
    for f in files:
        if os.path.exists(f):
            files_tmp.append(f)
        else:
            _LOG.warning("'%s' doesn't exist", f)
    return files_tmp


def get_modified_files():
    """
    Return the list of files that are added and modified. In other words the
    files that will be committed with a `git commit -am ...`.

    Equivalent to dev_scripts/git_files.sh
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
    _, files = si.system_to_string(cmd)
    files = files.split()
    files = _check_files(files)
    return files


def get_previous_committed_files(num_commits=1):
    """
    Return the list of files changed by the current git user in the last
    `num_commits` commits.

    Equivalent to dev_scripts/git_previous_commit_files.sh
    """
    cmd = []
    cmd.append('git show --pretty="" --name-only')
    cmd.append("$(git log --author $(git config user.name) -%d" % num_commits)
    cmd.append(r"""| \grep "^commit " | perl -pe 's/commit (.*)/$1/')""")
    cmd = " ".join(cmd)
    _, files = si.system_to_string(cmd)
    #
    files = files.split()
    files = sorted(list(set(files)))
    files = _check_files(files)
    return files


# #############################################################################


def git_log(num_commits=5, my_commits=False):
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
        cmd.append("--author $(git config user.name)")
    cmd = " ".join(cmd)
    _, txt = si.system_to_string(cmd)
    return txt


# #############################################################################


def git_stash_push(prefix, msg=None, log_level=logging.DEBUG):
    user_name = si.get_user_name()
    server_name = si.get_server_name()
    timestamp = hdt.get_timestamp()
    tag = "%s-%s-%s" % (user_name, server_name, timestamp)
    tag = prefix + "." + tag
    _LOG.debug("tag='%s'", tag)
    cmd = "git stash push"
    _LOG.debug("msg='%s'", msg)
    push_msg = tag[:]
    if msg:
        push_msg += ": " + msg
    cmd += " -m '%s'" % push_msg
    si.system(cmd, suppress_output=False, log_level=log_level)
    # Check if we actually stashed anything.
    cmd = r"git stash list | \grep '%s' | wc -l" % tag
    _, output = si.system_to_string(cmd)
    was_stashed = int(output) > 0
    if not was_stashed:
        msg = "Nothing was stashed"
        _LOG.warning(msg)
        # raise RuntimeError(msg)
    return tag, was_stashed


def git_stash_apply(mode, log_level=logging.DEBUG):
    _LOG.debug("# Checking stash head ...")
    cmd = "git stash list | head -3"
    si.system(cmd, suppress_output=False, log_level=log_level)
    #
    _LOG.debug("# Restoring local changes...")
    if mode == "pop":
        cmd = "git stash pop --quiet"
    elif mode == "apply":
        cmd = "git stash apply --quiet"
    else:
        raise ValueError("mode='%s'" % mode)
    si.system(cmd, suppress_output=False, log_level=log_level)
