import logging
import os
import re

import helpers.datetime_ as datetime_
import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# TODO(gp): Check https://git-scm.com/book/en/v2/Appendix-B%3A-Embedding-Git-in-your-Applications-Dulwich

# TODO(gp): Avoid "stuttering": the module is already called "git", so no need
# to make reference to git again.


def is_inside_submodule():
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


def get_client_root(super_module=True):
    """
    Return the full path of the root of the Git client.

    :param super_module: if True use the root of the Git supermodule, if we are
        in a submodule, otherwise use the Git submodule root

    E.g., "/Users/saggese/src/.../amp"
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


def get_path_from_git_root(file_name, super_module=True):
    """
    Get the git path from the root of the tree.
    """
    git_root = get_client_root(super_module=super_module) + "/"
    abs_path = os.path.abspath(file_name)
    dbg.dassert(abs_path.startswith(git_root))
    end_idx = len(git_root)
    ret = abs_path[end_idx:]
    # cmd = "git ls-tree --full-name --name-only HEAD %s" % file_name
    # _, git_file_name = si.system_to_string(cmd)
    # dbg.dassert_ne(git_file_name, "")
    return ret


def _check_files(files):
    files_tmp = []
    for f in files:
        if os.path.exists(f):
            files_tmp.append(f)
        else:
            _LOG.warning("'%s' doesn't exist", f)
    return files_tmp


def get_modified_files():
    """
    Equivalent to dev_scripts/git_files.sh
    """
    cmd = "(git diff --cached --name-only; git ls-files -m) | sort | uniq"
    _, files = si.system_to_string(cmd)
    files = files.split()
    files = _check_files(files)
    return files


def get_previous_committed_files(num_commits=1, uniquify=True):
    """
    Equivalent to dev_scripts/git_previous_commit_files.sh
    """
    cmd = (
        'git show --pretty="" --name-only'
        + " $(git log --author $(git config user.name) -%d " % num_commits
        + r"""| \grep "^commit " | perl -pe 's/commit (.*)/$1/')"""
    )
    _, files = si.system_to_string(cmd)
    files = files.split()
    if uniquify:
        files = sorted(list(set(files)))
    files = _check_files(files)
    return files


# TODO(gp): -> get_user_name()
def get_git_name():
    """
    Return the git user name.
    """
    cmd = "git config --get user.name"
    _, output = si.system_to_string(cmd)
    git_name = output.split("\n")
    dbg.dassert_eq(len(git_name), 1, "output='%s'", output)
    git_name = git_name[0]
    return git_name


def get_repo_symbolic_name():
    """
    Return the name of the repo like "alphamatic/amp".
    """
    cmd = "git remote -v | grep fetch"
    # TODO(gp): Make it more robust, by checking both fetch and push.
    # "origin  git@github.com:alphamatic/amp (fetch)"
    _, output = si.system_to_string(cmd)
    data = output.split()
    _LOG.debug("data=%s", data)
    dbg.dassert(len(data), 3, "data='%s'", data)
    # git@github.com:alphamatic/amp
    repo_name = data[1]
    m = re.match(r"^.*\.com:(.*)$", repo_name)
    dbg.dassert(m, "Can't parse '%s'", repo_name)
    repo_name = m.group(1)
    _LOG.debug("repo_name=%s", repo_name)
    # We expect something like "alphamatic/amp".
    m = re.match(r"^\S+/\S+$", repo_name)
    dbg.dassert(m, "repo_name='%s'", repo_name)
    # origin  git@github.com:ParticleDev/ORG_Particle.git (fetch)
    suffix_to_remove = ".git"
    if repo_name.endswith(suffix_to_remove):
        repo_name = repo_name[: -len(suffix_to_remove)]
    return repo_name


def git_stash_push(prefix=None, msg=None, log_level=logging.DEBUG):
    user_name = si.get_user_name()
    server_name = si.get_server_name()
    timestamp = datetime_.get_timestamp()
    tag = "wip.%s-%s-%s" % (user_name, server_name, timestamp)
    if prefix:
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
