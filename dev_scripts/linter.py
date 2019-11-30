#!/usr/bin/env python
"""
Reformat and lint python and ipynb files.

This script uses the version of the files present on the disk and not what is
staged for commit by git, thus you need to stage again after running it.

E.g.,
# Lint all modified files in git client.
> linter.py

# Lint current files.
> linter.py --current_git_files --collect_only
> linter.py --current_git_files --all

# Lint previous commit files.
> linter.py --previous_git_commit_files --collect_only

# Lint a certain number of previous commits
> linter.py --previous_git_commit_files n --collect_only
> linter.py --files event_study/*.py linter_v2.py --yapf --isort -v DEBUG

# Lint all python files, but not the notebooks.
> linter.py -d . --only_py --collect

# To jump to all the warnings to fix:
> vim -c "cfile linter.log"

# Check all jupytext files.
> linter.py -d . --action sync_jupytext
"""

import argparse
import itertools
import logging
import os
import py_compile
import re
import sys
from typing import Iterable, List

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# Use the current dir and not the dir of the executable.
_TMP_DIR = os.path.abspath(os.getcwd() + "/tmp.linter")


# #############################################################################
# Utils.
# #############################################################################


# TODO(gp): This could become the default behavior of system().
def _system(cmd, abort_on_error=True):
    suppress_output = _LOG.getEffectiveLevel() > logging.DEBUG
    rc = si.system(
        cmd,
        abort_on_error=abort_on_error,
        suppress_output=suppress_output,
        log_level=logging.DEBUG,
    )
    return rc


def _remove_empty_lines(output):
    output = [l for l in output if l.strip("\n") != ""]
    return output


def _filter_target_files(file_names):
    """
    Keep only the files that:
    - have extension .py, .ipynb, .txt or .md.
    - are not Jupyter checkpoints
    - are not in tmp dirs
    """
    file_names_out = []
    for file_name in file_names:
        _, file_ext = os.path.splitext(file_name)
        # We skip .ipynb since jupytext is part of the main flow.
        is_valid = file_ext in (".py", ".txt", ".md")
        is_valid &= ".ipynb_checkpoints/" not in file_name
        # Skip files in directory starting with "tmp.".
        is_valid &= "/tmp." not in file_name
        # Skip files starting with "tmp.".
        is_valid &= not file_name.startswith("tmp.")
        if is_valid:
            file_names_out.append(file_name)
    return file_names_out


# TODO(gp): Horrible: to remove / rewrite.
def _clean_file(file_name, write_back):
    """
    Remove empty spaces, tabs, windows end-of-lines.
    :param write_back: if True the file is overwritten in place.
    """
    # Read file.
    file_in = []
    with open(file_name, "r") as f:
        for line in f:
            file_in.append(line)
    #
    file_out = []
    for line in file_in:
        # A line can be deleted if it has only spaces and \n.
        if not any(char not in (" ", "\n") for char in line):
            line = "\n"
        # dos2unix.
        line = line.replace("\r\n", "\n")
        file_out.append(line)
    # Remove whitespaces at the end of file.
    while file_out and (file_out[-1] == "\n"):
        # While the last item in the list is blank, removes last element.
        file_out.pop(-1)
    # Write the new the output to file.
    if write_back:
        file_in = "".join(file_in)
        file_out = "".join(file_out)
        if file_in != file_out:
            _LOG.debug("Writing back file '%s'", file_name)
            with open(file_name, "w") as f:
                f.write(file_out)
        else:
            _LOG.debug("No change in file, so no saving")
    return file_in, file_out


def _annotate_output(output, executable):
    """
    Annotate a list containing the output of a cmd line with the name of the
    executable used.
    :return: list of strings
    """
    dbg.dassert_isinstance(output, list)
    output = [t + " [%s]" % executable for t in output]
    dbg.dassert_isinstance(output, list)
    return output


def _tee(cmd, executable, abort_on_error):
    """
    Execute command "cmd", capturing its output and removing empty lines.
    :return: list of strings
    """
    _LOG.debug("cmd=%s executable=%s", cmd, executable)
    _, output = si.system_to_string(cmd, abort_on_error=abort_on_error)
    dbg.dassert_isinstance(output, str)
    _LOG.debug("output1='\n%s'", output)
    output = output.split("\n")
    output = _remove_empty_lines(output)
    _LOG.debug("output2='\n%s'", "\n".join(output))
    dbg.dassert_isinstance(output, list)
    return output


# #############################################################################
# Handle files.
# #############################################################################


def _get_files(args) -> Iterable[str]:
    """
    Return the list of files to process given the command line arguments.
    """
    file_names: List[str] = []
    if args.files:
        _LOG.debug("Specified files")
        # User has specified files.
        file_names = args.files
    else:
        if args.previous_git_commit_files is not None:
            _LOG.debug("Looking for files committed in previous Git commit")
            # Get all the git in user previous commit.
            n_commits = args.previous_git_commit_files
            _LOG.info("Using %s previous commits", n_commits)
            file_names = git.get_previous_committed_files(n_commits)
        elif args.dir_name:
            if args.dir_name == "$GIT_ROOT":
                dir_name = git.get_client_root(super_module=True)
            else:
                dir_name = args.dir_name
            dir_name = os.path.abspath(dir_name)
            _LOG.info("Looking for all files in '%s'", dir_name)
            dbg.dassert_exists(dir_name)
            cmd = "find %s -name '*' -type f" % dir_name
            _, output = si.system_to_string(cmd)
            file_names = output.split("\n")
        if not file_names or args.current_git_files:
            # Get all the git modified files.
            file_names = git.get_modified_files()
    # Remove text files used in unit tests.
    file_names = [f for f in file_names if not is_test_input_output_file(f)]
    return file_names


def _get_files_to_lint(args, file_names: List[str]) -> List[str]:
    """
    Get all the files that need to be linted.

    Typically files to lint are python and notebooks.
    """
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    # Keep only actual .py and .ipynb files.
    file_names = _filter_target_files(file_names)
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    dbg.dassert_lte(
        1, len(file_names), "No files that can be linted are specified"
    )
    # Remove files.
    if args.skip_py:
        file_names = [f for f in file_names if not is_py_file(f)]
    if args.skip_ipynb:
        file_names = [f for f in file_names if not is_ipynb_file(f)]
    if args.skip_paired_jupytext:
        file_names = [f for f in file_names if not is_paired_jupytext_file(f)]
    # Keep files.
    if args.only_py:
        file_names = [
            f
            for f in file_names
            if is_py_file(f) and not is_paired_jupytext_file(f)
        ]
    if args.only_ipynb:
        file_names = [f for f in file_names if is_ipynb_file(f)]
    if args.only_paired_jupytext:
        file_names = [f for f in file_names if is_paired_jupytext_file(f)]
    #
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    if not file_names:
        msg = "No files were selected"
        _LOG.error(msg)
        raise ValueError(msg)
    return file_names


# #############################################################################
# Actions.
# #############################################################################

# We use the command line instead of API because:
# - some tools don't have a public API
# - this make easier to reproduce / test commands using the command lines and
#   then incorporate in the code
# - it allows to have clear control over options


def _check_exec(tool):
    """
    :return: True if the executables "tool" can be executed.
    """
    rc = _system("which %s" % tool, abort_on_error=False)
    return rc == 0


_THIS_MODULE = sys.modules[__name__]


def _get_action_func(action):
    """
    Return the function corresponding to the passed string.
    """
    # Dynamic dispatch doesn't work with joblib since this module is injected
    # in another module.
    # func_name = "_" + action
    # dbg.dassert(
    #        hasattr(_THIS_MODULE, func_name),
    #        msg="Invalid function '%s' in '%s'" % (func_name, _THIS_MODULE))
    # return getattr(_THIS_MODULE, func_name)
    map_ = {
        "autoflake": _autoflake,
        "basic_hygiene": _basic_hygiene,
        "black": _black,
        "flake8": _flake8,
        "ipynb_format": _ipynb_format,
        "isort": _isort,
        "lint_markdown": _lint_markdown,
        "mypy": _mypy,
        "pydocstyle": _pydocstyle,
        "pylint": _pylint,
        "pyment": _pyment,
        "python_compile": _python_compile,
        "sync_jupytext": _sync_jupytext,
        "test_jupytext": _test_jupytext,
        "yapf": _yapf,
    }
    return map_[action]


def _remove_not_possible_actions(actions):
    """
    Check whether each action in "actions" can be executed and return a list of
    the actions that can be executed.

    :return: list of strings representing actions
    """
    actions_tmp = []
    for action in actions:
        func = _get_action_func(action)
        is_possible = func(file_name=None, pedantic=None, check_if_possible=True)
        if not is_possible:
            _LOG.warning("Can't execute action '%s': skipping", action)
        else:
            actions_tmp.append(action)
    return actions_tmp


def _actions_to_string(actions):
    actions_as_str = [
        "%16s: %s" % (a, "Yes" if a in actions else "-") for a in _ALL_ACTIONS
    ]
    return "\n".join(actions_as_str)


def _test_actions():
    _LOG.info("Testing actions")
    # Check all the actions.
    num_not_poss = 0
    possible_actions = []
    for action in _ALL_ACTIONS:
        func = _get_action_func(action)
        is_possible = func(file_name=None, pedantic=False, check_if_possible=True)
        _LOG.debug("%s -> %s", action, is_possible)
        if is_possible:
            possible_actions.append(action)
        else:
            num_not_poss += 1
    # Report results.
    actions_as_str = _actions_to_string(possible_actions)
    _LOG.info("Possible actions:\n%s", pri.space(actions_as_str))
    if num_not_poss > 0:
        _LOG.warning("There are %s actions that are not possible", num_not_poss)
    else:
        _LOG.info("All actions are possible")


# ##############################################################################

# TODO(gp): We should use a Strategy pattern, having a base class and a class
#  for each action.

# Each action accepts:
# :param file_name: name of the file to process
# :param pendantic: True if it needs to be run in angry mode
# :param check_if_possible: check if the action can be executed on filename
# :return: list of strings representing the output


def _write_file_back(file_name: str, txt: Iterable[str], txt_new: Iterable[str]):
    txt = "\n".join(txt)
    txt_new = "\n".join(txt_new)
    if txt != txt_new:
        io_.to_file(file_name, txt_new)


def _basic_hygiene(file_name, pedantic, check_if_possible):
    _ = pedantic
    if check_if_possible:
        # We don't need any special executable, so we can always run this action.
        return True
    output = []
    # Read file.
    dbg.dassert(file_name)
    txt = io_.from_file(file_name, split=True)
    # Process file.
    txt_new = []
    for line in txt:
        if "\t" in line:
            msg = "Found tabs in %s: please use 4 spaces as per PEP8" % file_name
            _LOG.warning(msg)
            output.append(msg)
        # Convert tabs.
        line = line.replace("\t", " " * 4)
        # Remove trailing spaces.
        line = line.rstrip()
        # dos2unix.
        line = line.replace("\r\n", "\n")
        # TODO(gp): Remove empty lines in functions.
        txt_new.append(line.rstrip("\n"))
    # Remove whitespaces at the end of file.
    while txt_new and (txt_new[-1] == "\n"):
        # While the last item in the list is blank, removes last element.
        txt_new.pop(-1)
    # Write.
    _write_file_back(file_name, txt, txt_new)
    return output


def _python_compile(file_name, pedantic, check_if_possible):
    """
    Check that the code is valid python.
    """
    _ = pedantic
    if check_if_possible:
        return True
    #
    # Applicable only to python files.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    output = []
    try:
        py_compile.compile(file_name, doraise=True)
        # pylint: disable=broad-except
    except Exception as e:
        output.append(str(e))
    return output


def _autoflake(file_name, pedantic, check_if_possible):
    """
    Remove unused imports and variables.
    """
    _ = pedantic
    executable = "autoflake"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    opts = "-i --remove-all-unused-imports --remove-unused-variables"
    cmd = executable + " %s %s" % (opts, file_name)
    return _tee(cmd, executable, abort_on_error=False)


def _yapf(file_name, pedantic, check_if_possible):
    """
    Apply yapf code formatter.
    """
    _ = pedantic
    executable = "yapf"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    opts = "-i --style='google'"
    cmd = executable + " %s %s" % (opts, file_name)
    return _tee(cmd, executable, abort_on_error=False)


def _black(file_name, pedantic, check_if_possible):
    """
    Apply black code formatter.
    """
    _ = pedantic
    executable = "black"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    opts = "--line-length 82"
    cmd = executable + " %s %s" % (opts, file_name)
    output = _tee(cmd, executable, abort_on_error=False)
    # Remove the lines:
    # - reformatted core/test/test_core.py
    # - 1 file reformatted.
    # - All done!
    # - 1 file left unchanged.
    to_remove = ["All done!", "file left unchanged", "reformatted"]
    output = [l for l in output if all(w not in l for w in to_remove)]
    return output


def _isort(file_name, pedantic, check_if_possible):
    """
    Sort imports using isort.
    """
    _ = pedantic
    executable = "isort"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    cmd = executable + " %s" % file_name
    output = _tee(cmd, executable, abort_on_error=False)
    return output


def _flake8(file_name, pedantic, check_if_possible):
    """
    Look for formatting and semantic issues in code and docstrings.
    It relies on:
        - mccabe
        - pycodestyle
        - pyflakes
    """
    _ = pedantic
    executable = "flake8"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    # TODO(gp): Does -j 4 help?
    opts = "--exit-zero --doctests --max-line-length=82 -j 4"
    ignore = [
        # - "W503 line break before binary operator"
        #     - Disabled because in contrast with black formatting.
        "W503",
        # - W504 line break after binary operator
        #     - Disabled because in contrast with black formatting.
        "W504",
        # - E203 whitespace before ':'
        #     - Disabled because in contrast with black formatting
        "E203",
        # - E266 too many leading '#' for block comment
        #     - We have disabled this since it is a false positive for jupytext
        #       files.
        "E266,"
        # - E501 line too long (> 82 characters)
        #     - We have disabled this since it triggers also for docstrings at
        #       the beginning of the line. We let pylint pick the lines too
        #       long, since it seems to be smarter.
        "E501",
        # - E731 do not assign a lambda expression, use a def
        "E731",
        # - E265 block comment should start with '# '
        "E265",
    ]
    is_jupytext_code = is_paired_jupytext_file(file_name)
    _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
    if is_jupytext_code:
        ignore.extend(
            [
                # E501 line too long.
                "E501"
            ]
        )
    opts += " --ignore=" + ",".join(ignore)
    cmd = executable + " %s %s" % (opts, file_name)
    #
    output = _tee(cmd, executable, abort_on_error=True)
    # Remove some errors.
    is_jupytext_code = is_paired_jupytext_file(file_name)
    _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
    if is_jupytext_code:
        output_tmp = []
        for line in output:
            # F821 undefined name 'display' [flake8]
            if "F821" in line and "undefined name 'display'" in line:
                continue
            output_tmp.append(line)
        output = output_tmp
    return output


def _pydocstyle(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "pydocstyle"
    if check_if_possible:
        return _check_exec(executable)
    #
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    # http://www.pydocstyle.org/en/2.1.1/error_codes.html
    ignore = [
        # D105: Missing docstring in magic method
        "D105",
        # D200: One-line docstring should fit on one line with quotes
        "D200",
        # D202: No blank lines allowed after function docstring
        "D202",
        # D212: Multi-line docstring summary should start at the first line
        "D212",
        # D203: 1 blank line required before class docstring (found 0)
        "D203",
        # D205: 1 blank line required between summary line and description
        "D205",
        # D400: First line should end with a period (not ':')
        "D400",
        # D402: First line should not be the function's "signature"
        "D402",
        # D407: Missing dashed underline after section
        "D407",
        # D413: Missing dashed underline after section
        "D413",
        # D415: First line should end with a period, question mark, or
        # exclamation point
        "D415",
    ]
    if not pedantic:
        ignore.extend(
            [
                # D100: Missing docstring in public module
                "D100",
                # D101: Missing docstring in public class
                "D101",
                # D102: Missing docstring in public method
                "D102",
                # D103: Missing docstring in public function
                "D103",
                # D104: Missing docstring in public package
                "D104",
                # D107: Missing docstring in __init__
                "D107",
            ]
        )
    opts = ""
    if ignore:
        opts += "--ignore " + ",".join(ignore)
    # yapf: disable
    cmd = executable + " %s %s" % (opts, file_name)
    # yapf: enable
    # We don't abort on error on pydocstyle, since it returns error if there is
    # any violation.
    _, file_lines = si.system_to_string(cmd, abort_on_error=False)
    # Process lint_log transforming:
    #   linter_v2.py:1 at module level:
    #       D400: First line should end with a period (not ':')
    # into:
    #   linter_v2.py:1: at module level: D400: First line should end with a
    #   period (not ':')
    #
    output = []
    #
    file_lines = file_lines.split("\n")
    lines = ["", ""]
    for cnt, line in enumerate(file_lines):
        line = line.rstrip("\n")
        # _log.debug("line=%s", line)
        if cnt % 2 == 0:
            regex = r"(\s(at|in)\s)"
            subst = r":\1"
            line = re.sub(regex, subst, line)
        else:
            line = line.lstrip()
        # _log.debug("-> line=%s", line)
        lines[cnt % 2] = line
        if cnt % 2 == 1:
            line = "".join(lines)
            output.append(line)
    return output


def _pyment(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "pyment"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = "-w --first-line False -o reST"
    cmd = executable + " %s %s" % (opts, file_name)
    return _tee(cmd, executable, abort_on_error=False)


def _pylint(file_name, pedantic, check_if_possible):
    executable = "pylint"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python file.
    dbg.dassert(file_name)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = ""
    # We ignore these errors as too picky.
    ignore = [
        # [C0302(too-many-lines), ] Too many lines in module (/1000)
        "C0302",
        # [C0304(missing-final-newline), ] Final newline missing
        "C0304",
        # [C0330(bad-continuation), ] Wrong hanging indentation before block
        #   (add 4 spaces).
        # - Black and pylint don't agree on the formatting.
        "C0330",
        # [C0412(ungrouped-imports), ] Imports from package ... are not grouped
        "C0412",
        # [C0415(import-outside-toplevel), ] Import outside toplevel
        "C0415",
        # [R0903(too-few-public-methods), ] Too few public methods (/2)
        "R0903",
        # [R0912(too-many-branches), ] Too many branches (/12)
        "R0912",
        # R0913(too-many-arguments), ] Too many arguments (/5)
        "R0913",
        # [R0914(too-many-locals), ] Too many local variables (/15)
        "R0914",
        # [R0915(too-many-statements), ] Too many statements (/50)
        "R0915",
        # [W0123(eval-used), ] Use of eval
        "W0123",
        # [W0125(using-constant-test), ] Using a conditional statement with a
        #   constant value
        "W0125",
        # [W0511(fixme), ]
        "W0511",
        # [W0603(global-statement), ] Using the global statement
        # - We assume that we are mature enough to use `global` properly.
        "W0603",
        # [W1113(keyword-arg-before-vararg), ] Keyword argument before variable
        #   positional arguments list in the definition of
        # - TODO(gp): Not clear what is the problem.
        "W1113",
    ]
    is_test_code_tmp = is_under_test_dir(file_name)
    _LOG.debug("is_test_code_tmp=%s", is_test_code_tmp)
    if is_test_code_tmp:
        # TODO(gp): For files inside "test", disable:
        ignore.extend(
            [
                # [C0103(invalid-name), ] Class name "Test_dassert_eq1"
                #   doesn't conform to PascalCase naming style
                "C0103",
                # [R0201(no-self-use), ] Method could be a function
                "R0201",
                # [W0212(protected-access), ] Access to a protected member
                #   _get_default_tempdir of a client class
                "W0212",
            ]
        )
    is_jupytext_code = is_paired_jupytext_file(file_name)
    _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
    if is_jupytext_code:
        ignore.extend(
            [
                # [W0104(pointless-statement), ] Statement seems to have no effect
                # This is disabled since we use just variable names to print.
                "W0104",
                # [W0106(expression-not-assigned), ] Expression # ... is
                # assigned to nothing
                "W0106",
                # [W0621(redefined-outer-name), ] Redefining name ... from outer
                # scope
                "W0621",
            ]
        )
    if not pedantic:
        ignore.extend(
            [
                # [C0103(invalid-name), ] Constant name "..." doesn't conform to
                #   UPPER_CASE naming style
                "C0103",
                # [C0111(missing - docstring), ] Missing module docstring
                "C0111",
                # [C0301(line-too-long), ] Line too long (1065/100)
                "C0301",
            ]
        )
    if ignore:
        opts += "--disable " + ",".join(ignore)
    # Allow short variables, as long as camel-case.
    opts += ' --variable-rgx="[a-z0-9_]{1,30}$"'
    opts += ' --argument-rgx="[a-z0-9_]{1,30}$"'
    # TODO(gp): Not sure this is needed anymore.
    opts += " --ignored-modules=pandas"
    opts += " --output-format=parseable"
    # TODO(gp): Does -j 4 help?
    opts += " -j 4"
    cmd = executable + " %s %s" % (opts, file_name)
    output = _tee(cmd, executable, abort_on_error=False)
    # Remove some errors.
    output_tmp = []
    for line in output:
        if is_jupytext_code:
            # [E0602(undefined-variable), ] Undefined variable 'display'
            if "E0602" in line and "Undefined variable 'display'" in line:
                continue
        if line.startswith("Your code has been rated"):
            # Your code has been rated at 10.00/10 (previous run: ...
            line = file_name + ": " + line
        output_tmp.append(line)
    output = output_tmp
    # Remove lines.
    output = [l for l in output if ("-" * 20) not in l]
    # if output:
    #    output.insert(0, "* file_name=%s" % file_name)
    return output


def _mypy(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "mypy"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only python files, that are not paired with notebooks.
    dbg.dassert(file_name)
    if not is_py_file(file_name) or is_paired_jupytext_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    cmd = executable + " %s" % file_name
    _system(
        cmd,
        # mypy returns -1 if there are errors.
        abort_on_error=False,
    )
    output = _tee(cmd, executable, abort_on_error=False)
    # Remove some errors.
    output_tmp = []
    for line in output:
        if (
            line.startswith("Success:")
            or
            # Found 2 errors in 1 file (checked 1 source file)
            line.startswith("Found ")
            or
            # note: See https://mypy.readthedocs.io
            "note: See https" in line
        ):
            continue
        output_tmp.append(line)
    output = output_tmp
    # if output:
    #    output.insert(0, "* file_name=%s" % file_name)
    return output


# ##############################################################################


def _ipynb_format(file_name, pedantic, check_if_possible):
    _ = pedantic
    curr_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    executable = "%s/ipynb_format.py" % curr_path
    if check_if_possible:
        return _check_exec(executable)
    # Applicable to only ipynb file.
    dbg.dassert(file_name)
    if not is_ipynb_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    #
    cmd = executable + " %s" % file_name
    _system(cmd)
    output = []
    return output


# TODO(gp): Move in a more general file.
def _is_under_dir(file_name, dir_name):
    """
    Return whether a file is under the given directory.
    """
    subdir_names = file_name.split("/")
    return dir_name in subdir_names


def is_under_test_dir(file_name):
    """
    Return whether a file is under a test directory (which is called "test").
    """
    return _is_under_dir(file_name, "test")


def is_test_input_output_file(file_name):
    """
    Return whether a file is used as input or output in a unit test.
    """
    ret = is_under_test_dir(file_name)
    ret &= file_name.endswith(".txt")
    return ret


def is_test_code(file_name):
    ret = is_under_test_dir(file_name)
    ret &= os.path.basename(file_name).startswith("test_")
    ret &= file_name.endswith(".py")
    return ret


def is_py_file(file_name):
    """
    Return whether a file is a python file.
    """
    return file_name.endswith(".py")


def is_ipynb_file(file_name):
    """
    Return whether a file is a jupyter notebook file.
    """
    return file_name.endswith(".ipynb")


def from_python_to_ipynb_file(file_name):
    dbg.dassert(is_py_file(file_name))
    ret = file_name.replace(".py", ".ipynb")
    return ret


def from_ipynb_to_python_file(file_name):
    dbg.dassert(is_ipynb_file(file_name))
    ret = file_name.replace(".ipynb", ".py")
    return ret


def is_paired_jupytext_file(file_name):
    """
    Return whether a file is a paired jupytext file.
    """
    is_paired = (
        is_py_file(file_name)
        and os.path.exists(from_python_to_ipynb_file(file_name))
        or (
            is_ipynb_file(file_name)
            and os.path.exists(from_ipynb_to_python_file(file_name))
        )
    )
    return is_paired


def _sync_jupytext(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "process_jupytext.py"
    if check_if_possible:
        return _check_exec(executable)
    #
    dbg.dassert(file_name)
    # TODO(gp): Use the usual idiom of these functions.
    if is_py_file(file_name) and is_paired_jupytext_file(file_name):
        cmd = executable + " -f %s --action sync" % file_name
        output = _tee(cmd, executable, abort_on_error=True)
    else:
        _LOG.debug("Skipping file_name='%s'", file_name)
        output = []
    return output


def _test_jupytext(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "process_jupytext.py"
    if check_if_possible:
        return _check_exec(executable)
    #
    dbg.dassert(file_name)
    # TODO(gp): Use the usual idiom of these functions.
    if is_py_file(file_name) and is_paired_jupytext_file(file_name):
        cmd = executable + " -f %s --action test" % file_name
        output = _tee(cmd, executable, abort_on_error=True)
    else:
        _LOG.debug("Skipping file_name='%s'", file_name)
        output = []
    return output


# ##############################################################################


def _lint_markdown(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "prettier"
    if check_if_possible:
        return _check_exec(executable)
    # Applicable only to txt and md files.
    dbg.dassert(file_name)
    ext = os.path.splitext(file_name)[1]
    output = []
    if ext not in (".txt", ".md"):
        _LOG.debug("Skipping file_name='%s' because ext='%s'", file_name, ext)
        return output
    #
    # Pre-process text.
    #
    txt = io_.from_file(file_name, split=True)
    txt_new = []
    for line in txt:
        line = re.sub(r"^\* ", "- STAR", line)
        txt_new.append(line)
    # Write.
    txt_new = "\n".join(txt_new)
    io_.to_file(file_name, txt_new)
    #
    # Lint.
    #
    cmd_opts = []
    cmd_opts.append("--parser markdown")
    cmd_opts.append("--prose-wrap always")
    cmd_opts.append("--write")
    cmd_opts.append("--tab-width 4")
    cmd_opts = " ".join(cmd_opts)
    cmd = " ".join([executable, cmd_opts, file_name])
    output_tmp = _tee(cmd, executable, abort_on_error=True)
    output.extend(output_tmp)
    #
    # Post-process text.
    #
    txt = io_.from_file(file_name, split=True)
    txt_new = []
    for i, line in enumerate(txt):
        # Check whether there is TOC otherwise add it.
        if i == 0 and line != "<!--ts-->":
            output.append("No tags for table of content in md file: adding it")
            txt_new.append("<!--ts-->\n<!--te-->")
        line = re.sub(r"^\-   STAR", "*   ", line)
        # Remove some artifacts when copying from gdoc.
        line = re.sub("’", "'", line)
        line = re.sub("“", '"', line)
        line = re.sub("”", '"', line)
        line = re.sub("…", "...", line)
        # -   You say you'll do something
        # line = re.sub("^(\s*)-   ", r"\1- ", line)
        # line = re.sub("^(\s*)\*   ", r"\1* ", line)
        txt_new.append(line)
    # Write.
    txt_new = "\n".join(txt_new)
    io_.to_file(file_name, txt_new)
    #
    # Refresh table of content.
    #
    amp_path = git.get_amp_abs_path()
    cmd = []
    cmd.append(os.path.join(amp_path, "scripts/gh-md-toc"))
    cmd.append("--insert %s" % file_name)
    cmd = " ".join(cmd)
    _system(cmd, abort_on_error=False)
    return output


# #############################################################################


def _lint(file_name, actions, pedantic, debug):
    """
    Execute all the actions on a filename.

    Note that this is the unit of parallelization, i.e., we run all the
    actions on a single file to ensure that the actions are executed in the
    proper order.
    """
    output = []
    _LOG.info("\n%s", pri.frame(file_name, char1="="))
    for action in actions:
        _LOG.debug("\n%s", pri.frame(action, char1="-"))
        print("## %-20s (%s)" % (action, file_name))
        if debug:
            # Make a copy after each action.
            dst_file_name = file_name + "." + action
            cmd = "cp -a %s %s" % (file_name, dst_file_name)
            os.system(cmd)
        else:
            dst_file_name = file_name
        func = _get_action_func(action)
        # We want to run the stages, and not check.
        output_tmp = func(dst_file_name, pedantic, check_if_possible=False)
        # Annotate with executable [tag].
        output_tmp = _annotate_output(output_tmp, action)
        dbg.dassert_isinstance(
            output_tmp, list, msg="action=%s file_name=%s" % (action, file_name)
        )
        output.extend(output_tmp)
        if output_tmp:
            _LOG.info("\n%s", "\n".join(output_tmp))
    return output


def _select_actions(args):
    # Select phases.
    actions = args.action
    if isinstance(actions, str) and " " in actions:
        actions = actions.split(" ")
    if not actions or args.all:
        actions = _ALL_ACTIONS[:]
    if args.quick:
        actions = [a for a in _ALL_ACTIONS if a != "pylint"]
    # Validate actions.
    actions = set(actions)
    for action in actions:
        if action not in _ALL_ACTIONS:
            raise ValueError("Invalid action '%s'" % action)
    # Reorder actions according to _ALL_ACTIONS.
    actions_tmp = []
    for action in _ALL_ACTIONS:
        if action in actions:
            actions_tmp.append(action)
    actions = actions_tmp
    # Find the tools that are available.
    actions = _remove_not_possible_actions(actions)
    actions_as_str = _actions_to_string(actions)
    _LOG.info("# Action selected:\n%s", pri.space(actions_as_str))
    return actions


def _run_linter(actions, args, file_names):
    num_steps = len(file_names) * len(actions)
    _LOG.info(
        "Num of files=%d, num of actions=%d -> num of steps=%d",
        len(file_names),
        len(actions),
        num_steps,
    )
    pedantic = args.pedantic
    num_threads = args.num_threads
    # Use serial mode if there is a single file, unless the user specified
    # explicitly the numer of threads to use.
    if len(file_names) == 1 and num_threads != -1:
        num_threads = "serial"
        _LOG.warning(
            "Using num_threads='%s' since there is a single file", num_threads
        )
    if num_threads == "serial":
        output = []
        for file_name in file_names:
            output_tmp = _lint(file_name, actions, pedantic, args.debug)
            output.extend(output_tmp)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info(
            "Using %s threads", num_threads if num_threads > 0 else "all CPUs"
        )
        from joblib import Parallel, delayed

        output_tmp = Parallel(n_jobs=num_threads, verbose=50)(
            delayed(_lint)(file_name, actions, pedantic, args.debug)
            for file_name in file_names
        )
        output = list(itertools.chain.from_iterable(output_tmp))
    output.insert(0, "cmd line='%s'" % dbg.get_command_line())
    # TODO(gp): datetime_.get_timestamp().
    # output.insert(1, "datetime='%s'" % datetime.datetime.now())
    output = _remove_empty_lines(output)
    return output


# #############################################################################
# Main.
# #############################################################################

# Actions and if they read / write files.
# The order of this list implies the order in which they are executed.
_VALID_ACTIONS_META = [
    ("basic_hygiene", "w", "Clean up (e.g., tabs, trailing spaces)."),
    ("python_compile", "r", "Check that python code is valid"),
    (
        "autoflake",
        "w",
        "Removes unused imports and unused variables as reported by pyflakes.",
    ),
    (
        "isort",
        "w",
        "Sort Python import definitions alphabetically within logical sections.",
    ),
    # Superseded by black.
    # ("yapf", "w",
    #    "Formatter for Python code."),
    ("black", "w", "The uncompromising code formatter."),
    ("flake8", "r", "Tool For Style Guide Enforcement."),
    ("pydocstyle", "r", "Docstring style checker."),
    # Not installable through conda.
    # ("pyment", "w",
    #   "Create, update or convert docstring."),
    ("pylint", "w", "Check that module(s) satisfy a coding standard."),
    ("mypy", "r", "Static code analyzer using the hint types."),
    ("sync_jupytext", "w", "Create / sync jupytext files."),
    ("test_jupytext", "r", "Test jupytext files."),
    # Superseded by "sync_jupytext".
    # ("ipynb_format", "w",
    #   "Format jupyter code using yapf."),
    ("lint_markdown", "w", "Lint txt/md markdown files"),
]

_ALL_ACTIONS = list(zip(*_VALID_ACTIONS_META))[0]


def _main(args):
    dbg.init_logger(args.log_level)
    #
    if args.test_actions:
        _LOG.warning("Testing actions...")
        _test_actions()
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    output = []
    # Get all the files to process.
    all_file_names = _get_files(args)
    _LOG.info("Found %s files to process", len(all_file_names))
    # Select files.
    file_names = _get_files_to_lint(args, all_file_names)
    _LOG.info(
        "# Processing %d files:\n%s",
        len(file_names),
        pri.space("\n".join(file_names)),
    )
    if args.collect_only:
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    actions = _select_actions(args)
    # Create tmp dir.
    io_.create_dir(_TMP_DIR, incremental=False)
    _LOG.info("tmp_dir='%s'", _TMP_DIR)
    # Run linter.
    output_tmp = _run_linter(actions, args, file_names)
    dbg.dassert_isinstance(output_tmp, list)
    output.extend(output_tmp)
    # Sort the errors.
    output = sorted(output)
    # Print linter output.
    print(pri.frame(args.linter_log, char1="/").rstrip("\n"))
    print("\n".join(output) + "\n")
    print(pri.line(char="/").rstrip("\n"))
    # Write file.
    output = "\n".join(output)
    io_.to_file(args.linter_log, output)
    # Compute the number of lints.
    num_lints = 0
    for line in output.split("\n"):
        # dev_scripts/linter.py:493: ... [pydocstyle]
        if re.search(r"\S+:\d+.*\[\S+\]", line):
            num_lints += 1
    _LOG.info("num_lints=%d", num_lints)
    if num_lints != 0:
        _LOG.info(
            "You can quickfix the issues with\n> vim -c 'cfile %s'",
            args.linter_log,
        )
    #
    if not args.no_cleanup:
        io_.delete_dir(_TMP_DIR)
    else:
        _LOG.warning("Leaving tmp files in '%s'", _TMP_DIR)
    if args.jenkins:
        _LOG.warning("Skipping returning an error because of --jenkins")
        num_lints = 0
    return num_lints


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Select files.
    parser.add_argument(
        "-f", "--files", nargs="+", type=str, help="Files to process"
    )
    parser.add_argument(
        "-c",
        "--current_git_files",
        action="store_true",
        help="Select all files modified in the current git client",
    )
    parser.add_argument(
        "-p",
        "--previous_git_commit_files",
        nargs="?",
        type=int,
        const=1,
        default=None,
        help="Select all files modified in previous 'n' user git commit",
    )
    parser.add_argument(
        "-d",
        "--dir_name",
        action="store",
        help="Select all files in a dir. 'GIT_ROOT' to select git root",
    )
    # Select files based on type.
    parser.add_argument(
        "--skip_py", action="store_true", help="Do not process python scripts"
    )
    parser.add_argument(
        "--skip_ipynb",
        action="store_true",
        help="Do not process jupyter notebooks",
    )
    parser.add_argument(
        "--skip_paired_jupytext",
        action="store_true",
        help="Do not process paired notebooks",
    )
    parser.add_argument(
        "--only_py",
        action="store_true",
        help="Process only python scripts excluding paired notebooks",
    )
    parser.add_argument(
        "--only_ipynb", action="store_true", help="Process only jupyter notebooks"
    )
    parser.add_argument(
        "--only_paired_jupytext",
        action="store_true",
        help="Process only paired notebooks",
    )
    # Debug.
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Generate one file per transformation",
    )
    parser.add_argument(
        "--no_cleanup", action="store_true", help="Do not clean up tmp files"
    )
    parser.add_argument("--jenkins", action="store_true", help="Run as jenkins")
    # Test.
    parser.add_argument(
        "--collect_only",
        action="store_true",
        help="Print only the files to process and stop",
    )
    parser.add_argument(
        "--test_actions", action="store_true", help="Print the possible actions"
    )
    # Select actions.
    parser.add_argument("--action", action="append", help="Run a specific check")
    parser.add_argument(
        "--quick", action="store_true", help="Run all quick phases"
    )
    parser.add_argument(
        "--all", action="store_true", help="Run all recommended phases"
    )
    parser.add_argument(
        "--pedantic", action="store_true", help="Run some purely cosmetic lints"
    )
    parser.add_argument(
        "--num_threads",
        action="store",
        default="-1",
        help="Number of threads to use ('serial' to run serially, -1 to use "
        "all CPUs)",
    )
    #
    parser.add_argument(
        "--linter_log",
        default="./linter_warnings.txt",
        help="File storing the warnings",
    )
    prsr.add_verbosity_arg(parser)
    args = parser.parse_args()
    rc = _main(args)
    sys.exit(rc)


if __name__ == "__main__":
    _parse()
