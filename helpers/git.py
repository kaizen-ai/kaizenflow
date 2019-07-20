import logging
import os

import helpers.dbg as dbg
import helpers.system_interaction as hsi

_LOG = logging.getLogger(__name__)

# TODO(gp): Check https://git-scm.com/book/en/v2/Appendix-B%3A-Embedding-Git-in-your-Applications-Dulwich


def get_client_root():
    cmd = "git rev-parse --show-toplevel"
    _, out = hsi.system_to_string(cmd)
    out = out.rstrip("\n")
    dbg.dassert_eq(len(out.split("\n")), 1, msg="Invalid out='%s'" % out)
    client_root = os.path.realpath(out)
    return client_root


def get_path_from_git_root(file_name):
    """
    Get the git path from the root of the tree.

    :param file_name:
    :return:
    """
    cmd = "git ls-tree --full-name --name-only HEAD %s" % file_name
    _, git_file_name = hsi.system_to_string(cmd)
    dbg.dassert_ne(git_file_name, "")
    return git_file_name


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
    _, files = hsi.system_to_string(cmd)
    files = files.split()
    files = _check_files(files)
    return files


def get_previous_committed_files(num_commits=1, uniquify=True):
    """
    Equivalent to dev_scripts/git_previous_commit_files.sh
    """
    cmd = ('git show --pretty="" --name-only' +
           " $(git log --author $(git config user.name) -%d " % num_commits +
           r"""| \grep commit | perl -pe 's/commit (.*)/$1/')""")
    _, files = hsi.system_to_string(cmd)
    files = files.split()
    if uniquify:
        files = sorted(list(set(files)))
    files = _check_files(files)
    return files


def get_git_name():
    cmd = "git config --get user.name"
    git_name = hsi.system_to_string(cmd)[1]
    return git_name
