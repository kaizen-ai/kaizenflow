# NOTE: This file should depend only on Python standard libraries.
import inspect
import logging
import os
import subprocess
import sys
from typing import List, Tuple

_LOG = logging.getLogger(__name__)

# The path to the git-binary:
_GIT_BINARY_PATH = "git"

# The maximum file-size in KB for a file (not notebook) to be committed.
_MAX_FILE_SIZE_IN_KB = 512


# Stat copy-paste from helpers/printing.py
_COLOR_MAP = {
    "blue": 94,
    "green": 92,
    "white": 0,
    "purple": 95,
    "red": 91,
    "yellow": 33,
    # Blu.
    "DEBUG": 34,
    # Cyan.
    "INFO": 36,
    # Yellow.
    "WARNING": 33,
    # Red.
    "ERROR": 31,
    # White on red background.
    "CRITICAL": 41,
}


def color_highlight(text: str, color: str) -> str:
    """
    Return a colored string.
    """
    prefix = "\033["
    suffix = "\033[0m"
    assert color in _COLOR_MAP
    color_code = _COLOR_MAP[color]
    txt = f"{prefix}{color_code}m{text}{suffix}"
    return txt


# End copy-paste.


# Start copy-paste from helpers/introspection.py
def get_function_name(count: int = 0) -> str:
    """
    Return the name of the function calling this function, i.e., the name of
    the function calling `get_function_name()`.
    """
    ptr = inspect.currentframe()
    # count=0 corresponds to the calling function, so we need to add an extra
    # step walking the call stack.
    count += 1
    for _ in range(count):
        assert ptr is not None
        ptr = ptr.f_back
    func_name = ptr.f_code.co_name  # type: ignore
    return func_name


# End copy-paste.


# Start copy-paste from helpers/system_interaction.py
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


# End copy-paste.

# #############################################################################
# check_master
# #############################################################################


def _report() -> str:
    func_name = get_function_name(count=1)
    print("\n" + color_highlight(f"##### {func_name} ######", "purple"))
    return func_name


def _handle_error(func_name: str, error: bool, abort_on_error: bool) -> None:
    if error:
        print("\n" + color_highlight(f"'{func_name}' failed", "red"))
        if abort_on_error:
            sys.exit(-1)
    else:
        print(color_highlight(f"'{func_name}' passed", "green"))


def check_master(abort_on_error: bool = True) -> None:
    """
    Check if we are committing directly to master, instead of a branch.
    """
    func_name = _report()
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
        error = True
    else:
        error = False
    # Handle error.
    _handle_error(func_name, error, abort_on_error)


# #############################################################################
# check_author
# #############################################################################


def check_author(abort_on_error: bool = True) -> None:
    """
    Ensure that the committer use a gmail and not a corporate account.

    Extremely custom but effective invariant.
    """
    func_name = _report()
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
    print(f"user_email='{user_email}")
    # Check.
    error = False
    if not user_email.endswith("@gmail.com"):
        _LOG.error("user_email='%s' is incorrect", user_email)
        error = True
    # Handle error.
    _handle_error(func_name, error, abort_on_error)


# #############################################################################
# check_file_size
# #############################################################################


# Start copy-paste From helpers/introspection.py
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


# End copy-paste.


def check_file_size(abort_on_error: bool = True) -> None:
    """
    Ensure that (not notebook) files are not larger than a certain size.

    The goal is to avoid to check in a 100MB file.
    """
    func_name = _report()
    print(f"max file size={_MAX_FILE_SIZE_IN_KB} KB")
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
    error = False
    for file_name in file_list:
        if os.path.exists(file_name):
            stat = os.stat(file_name)
            size = stat.st_size
            size_as_str = _sizeof_fmt(stat.st_size)
            _LOG.debug("%s: %s", file_name, size_as_str)
            if not file_name.endswith(".ipynb"):
                if size > _MAX_FILE_SIZE_IN_KB * 1024:
                    # File is to big, abort the commit.
                    msg = (
                        f"Filename '{file_name}' is too big to be committed"
                        + f": {size_as_str} > {_MAX_FILE_SIZE_IN_KB} KB"
                    )
                    _LOG.error(msg)
                    error = True
    # Handle error.
    _handle_error(func_name, error, abort_on_error)


# #############################################################################


def check_forbidden_words(abort_on_error: bool = True) -> None:
    """
    Check that certain words are not used.
    """
    # Use Caesar cipher from https://stackoverflow.com/questions/8886947/caesar-cipher-function-in-python/54590077#54590077
    #    # TODO(gp): grep for EG
    #    jackpy "Eg"
    #    jackpy "EG" | grep -v PEG | grep -v TEG | grep -v AWS | grep -v ECR | grep -v TELEGRAM
    #    jackpy "egid"
    #    jackpy "lime"
    #    jackpy "crypto"
    #
    #    ffind.py lime | grep -v ".git"
    #    ffind.py e.g.,
