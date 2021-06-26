#!/usr/bin/env python3

"""
This is a git commit-hook used to check if:

- if we are committing to `master` directly
- if the author / email was set properly
- files in the staging area larger than `_MAX_FILE_SIZE_IN_KB` variable.

- In case of violations the script will exit non-zero and abort the commit.
"""

# TODO(gp): Check these hooks
# https://github.com/pre-commit/pre-commit-hooks/tree/master/pre_commit_hooks
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_ast.py
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_added_large_files.py
# https://github.com/pre-commit/pre-commit-hooks/blob/master/pre_commit_hooks/check_merge_conflict.py
# https://code-maven.com/enforcing-commit-message-format-in-git

import logging
import os
import subprocess
import sys
from typing import List, Tuple

_LOG = logging.getLogger(__name__)

# The path to the git-binary:
_GIT_BINARY_PATH = "git"

# The maximum file-size in KB (= 1024 byte) for a file to be committed:
_MAX_FILE_SIZE_IN_KB = 512


def _system_to_string(
    cmd: str, abort_on_error: bool = True, verbose: bool = False
) -> Tuple[int, str]:
    assert isinstance(cmd, str), "Type of '%s' is %s" % (str(cmd), type(cmd))
    if verbose:
        print(f"> {cmd}")
    stdout = subprocess.PIPE
    stderr = subprocess.STDOUT
    with subprocess.Popen(
        cmd, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr
    ) as p:
        output = ""
        while True:
            line = p.stdout.readline().decode("utf-8")  # type: ignore
            if not line:
                break
            print((line.rstrip("\n")))
            output += line
        p.stdout.close()  # type: ignore
        rc = p.wait()
    if abort_on_error and rc != 0:
        msg = (
            "cmd='%s' failed with rc='%s'" % (cmd, rc)
        ) + "\nOutput of the failing command is:\n%s" % output
        _LOG.error(msg)
        sys.exit(-1)
    return rc, output


# #############################################################################


def _check_master() -> None:
    print("\n## check_master")
    # Print some information.
    verbose = True
    cmd = "git rev-parse --abbrev-ref HEAD"
    rc, branch_name = _system_to_string(cmd, verbose=verbose)
    _ = rc
    branch_name = branch_name.lstrip().rstrip()
    print(f"Branch is '{branch_name}'")
    if branch_name == "master":
        msg = (
            "You shouldn't merge into `master`: please do a PR and then merge it"
        )
        _LOG.error(msg)
        sys.exit(-1)


# #############################################################################


def _check_author() -> None:
    print("\n## check_author")
    # Print some information.
    verbose = True
    var = "user.name"
    cmd = f"{_GIT_BINARY_PATH} config {var}"
    _system_to_string(cmd, verbose=verbose)
    cmd = f"{_GIT_BINARY_PATH} config --show-origin {var}"
    _system_to_string(cmd, verbose=verbose)
    #
    var = "user.email"
    cmd = f"{_GIT_BINARY_PATH} config {var}"
    rc, user_email = _system_to_string(cmd, verbose=verbose)
    _ = rc
    user_email = user_email.lstrip().rstrip()
    cmd = f"{_GIT_BINARY_PATH} config --show-origin {var}"
    _system_to_string(cmd, verbose=verbose)
    # Check.
    if not user_email.endswith("@gmail.com"):
        _LOG.error("user_email='%s' is incorrect", user_email)
        sys.exit(-1)
    print(f"user_email='{user_email}' is valid")


# #############################################################################


# From helpers/introspection.py
def _sizeof_fmt(num: float) -> str:
    """
    Return a human-readable string for a filesize (e.g., "3.5 MB").
    """
    # From http://stackoverflow.com/questions/1094841
    for x in ["bytes", "KB", "MB", "GB", "TB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
    assert 0, "Invalid num='%s'" % num


def _check_file_size() -> None:
    print(f"\n## check_file_size: max file size={_MAX_FILE_SIZE_IN_KB} KB")
    # Check all files in the staging-area, i.e., everything but un-staged files.
    # TODO(gp): Check only staged files.
    cmd = f"{_GIT_BINARY_PATH} status --porcelain -uno"
    rc, txt = _system_to_string(cmd)
    _ = rc
    file_list: List[str] = txt.splitlines()
    # > git status --porcelain -uno
    #  M dev_scripts/git/git_hooks/pre-commit.py
    file_list = [file_name[3:] for file_name in file_list]
    # Check all files:
    for file_name in file_list:
        if os.path.exists(file_name):
            stat = os.stat(file_name)
            size = stat.st_size
            size_as_str = _sizeof_fmt(stat.st_size)
            print(f"{file_name}: {size_as_str}")
            if size > _MAX_FILE_SIZE_IN_KB * 1024:
                # File is to big, abort the commit.
                msg = "Filename '{file_name}' is too big to be committed: exiting"
                _LOG.error(msg)
                sys.exit(-1)
    # Everything seams to be okay.
    print("No huge files found.")


# #############################################################################


if __name__ == "__main__":
    print("# Running git pre-commit hook ...")
    _check_master()
    _check_author()
    _check_file_size()
    sys.exit(0)
