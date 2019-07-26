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

- To jump to all the warnings to fix:
> vim -c "cfile linter.log"

- Check all jupytext files.
> linter.py -d edgar --skip_py --action check_jupytext

# Run some test with:
> test_linter.sh
"""

# TODO(gp): Add mccabe score.
# TODO(gp): Do not overwrite file when there is no change.
# TODO(gp): Add autopep8 if useful?
# TODO(gp): Use Dask to parallelize
# TODO(gp): Add vulture, snake_food
# TODO(gp): It would be ideal to do two commits, but not sure how to do it
# TODO(gp): Save tarball, dir or patch of changes
# TODO(gp): Make sure all the py files can be python compiled
# TODO(gp): Check jupytext consistency (check_jupytext)
# TODO(gp): Lint notebooks and then apply jupytext (lint_ipynb)
# TODO(gp): Refresh all py files from ipynb
# TODO(gp): Ensure all file names are correct (e.g., no space, nice TaskXYZ, no - but _...)
# TODO(gp): Notebooks only in notebook dir
# TODO(gp): Make sure that there are no conflict markers.
# TODO(gp): Report number of errors vs warnings.
# TODO(gp): Test directory should be called tests and not test
# TODO(gp): Discourage checking in master

# Lint all py files that are not paired.
# Lint all paired py files and run jupytest --update.
# Lint all ipynb files and run jupytext.

import argparse
import datetime
import logging
import os
import re
import sys

import tqdm

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as print_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

_TMP_DIR = os.path.abspath("./tmp.linter")

# #############################################################################
# Utils.
# #############################################################################

# Sample at the beginning of time before we start fiddling with command line
# args.
_CMD_LINE = " ".join(arg for arg in sys.argv)


def _get_command_line():
    return _CMD_LINE


def _system(cmd, abort_on_error=True):
    # print _log.getEffectiveLevel(), logging.DEBUG
    suppress_output = _LOG.getEffectiveLevel() > logging.DEBUG
    # print "suppress_output=", suppress_output
    _LOG.getEffectiveLevel()
    rc = si.system(
        cmd,
        abort_on_error=abort_on_error,
        suppress_output=suppress_output,
        log_level=logging.DEBUG)
    return rc


def _remove_empty_lines(output):
    output = [l for l in output if l.strip("\n") != ""]
    return output


def _clean_file(filename, write_back):

    def is_all_whitespace(line):
        """
        Check if the line can be deleted.
        """
        for char in line:
            if char not in (' ', '\n'):
                return False
        return True

    with open(filename, 'r') as f:
        file_out = []
        for line in f:
            if is_all_whitespace(line):
                line = '\n'
            # dos2unix.
            line = line.replace('\r\n', '\n')
            file_out.append(line)
    # Removes whitespaces at the end of file.
    while file_out and (file_out[-1] == '\n'):
        # While the last item in lst is blank, removes last element.
        file_out.pop(-1)
    # Writes the new the output to file.
    if write_back:
        with open(filename, 'w') as f:
            f.write(''.join(file_out))
    return file_out


def _annotate(output, executable):
    dbg.dassert_isinstance(output, list)
    output = [t + " [%s]" % executable for t in output]
    dbg.dassert_isinstance(output, list)
    return output


def _tee(cmd, executable, abort_on_error):
    _LOG.debug("cmd=%s executable=%s", cmd, executable)
    _, output = si.system_to_string(cmd, abort_on_error=abort_on_error)
    dbg.dassert_isinstance(output, str)
    _LOG.debug("output1='\n%s'", output)
    output = output.split("\n")
    output = _remove_empty_lines(output)
    _LOG.debug("output2='\n%s'", "\n".join(output))
    return output


# #############################################################################
# Handle files.
# #############################################################################


def _get_files(args):
    """
    Return the list of files to process given the command line arguments.
    """
    file_names = []
    if args.files:
        # Files are specified.
        file_names = args.files
    else:
        if args.previous_git_commit_files is not None:
            # Get all the git in user previous commit.
            n_commits = args.previous_git_commit_files
            _LOG.info("Using %s previous commits", n_commits)
            file_names = git.get_previous_committed_files(n_commits)
        elif args.dir_name:
            if args.dir_name == "GIT_ROOT":
                dir_name = git.get_client_root()
            else:
                dir_name = args.dir_name
            dir_name = os.path.abspath(dir_name)
            _LOG.info("Looking for all files in '%s'", dir_name)
            dbg.dassert_exists(dir_name)
            cmd = "find %s -name '*' -type f" % dir_name
            output = si.system_to_string(cmd)[1]
            file_names = output.split("\n")
        if not file_names or args.current_git_files:
            # Get all the git modified files.
            file_names = git.get_modified_files()
    # Remove files.
    if args.skip_py:
        file_names = [f for f in file_names if not is_py_file(f)]
    if args.skip_ipynb:
        file_names = [f for f in file_names if not is_ipynb_file(f)]
    if args.skip_jupytext:
        file_names = [f for f in file_names if not is_paired_jupytext_file(f)]
    # Keep files.
    if args.only_py:
        file_names = [f for f in file_names if is_py_file(f)]
    if args.only_ipynb:
        file_names = [f for f in file_names if is_ipynb_file(f)]
    if args.only_jupytext:
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

# We use the command line instead of API because
# - some tools don't have a public API
# - make easier to reproduce / test commands with command lines and then
#   incorporate in the code
# - have clear control over options


def _check_exec(tool):
    """
    Return True if the executables "tool" can be executed.
    """
    rc = _system("which %s" % tool, abort_on_error=False)
    return rc == 0


_THIS_MODULE = sys.modules[__name__]


def _get_action_func(action):
    func_name = "_" + action
    dbg.dassert(
        hasattr(_THIS_MODULE, func_name),
        msg="Invalid function '%s'" % func_name)
    return getattr(_THIS_MODULE, func_name)


def _remove_not_possible_actions(actions):
    actions_tmp = []
    for action in actions:
        func = _get_action_func(action)
        is_possible = func(
            file_name=None, pedantic=False, check_if_possible=True)
        if not is_possible:
            _LOG.warning("Can't execute action '%s': skipping", action)
        else:
            actions_tmp.append(action)
    return actions_tmp


def _test_actions():
    _LOG.info("Testing actions")
    num_not_poss = 0
    for action in _VALID_ACTIONS:
        func = _get_action_func(action)
        is_possible = func(
            file_name=None, pedantic=False, check_if_possible=True)
        print("%s -> %s" % (action, is_possible))
        if not is_possible:
            num_not_poss += 1
    if num_not_poss > 0:
        _LOG.warning("There are %s actions that are not possible", num_not_poss)
    else:
        _LOG.info("All actions are possible")


# ##############################################################################


def _basic_hygiene(file_name, pedantic, check_if_possible):
    _ = pedantic
    if check_if_possible:
        # We don't need any special executable, so we can always run this action.
        return True
    output = []
    # Read file.
    dbg.dassert_ne(file_name, "")
    txt = _clean_file(file_name, write_back=False)
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
        txt_new.append(line.rstrip("\n"))
        # TODO(gp): Remove empty lines in functions.
    # Write.
    with open(file_name, 'w') as f:
        f.write("\n".join(txt_new))
    #
    return output


def _autoflake(file_name, pedantic, check_if_possible):
    """
    Remove unused imports and variables.
    """
    _ = pedantic
    executable = "autoflake"
    if check_if_possible:
        return _check_exec(executable)
    dbg.dassert_ne(file_name, "")
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = "-i --remove-all-unused-imports --remove-unused-variables"
    cmd = executable + " %s %s" % (opts, file_name)
    _system(cmd, abort_on_error=True)
    return []


def _yapf(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "yapf"
    if check_if_possible:
        return _check_exec(executable)
    dbg.dassert_ne(file_name, "")
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = "-i --style='google'"
    cmd = executable + " %s %s" % (opts, file_name)
    _system(cmd, abort_on_error=True)
    return []


def _isort(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "isort"
    if check_if_possible:
        return _check_exec(executable)
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    cmd = executable + " %s" % file_name
    _system(cmd, abort_on_error=True)
    return []


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
    dbg.dassert_ne(file_name, "")
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = "--exit-zero --doctests --max-line-length=80 -j 4"
    # E265 block comment should start with '# '
    opts += " --ignore=E265"
    cmd = executable + " %s %s" % (opts, file_name)
    return _tee(cmd, executable, abort_on_error=True)


def _pydocstyle(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "pydocstyle"
    if check_if_possible:
        return _check_exec(executable)
    dbg.dassert_ne(file_name, "")
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    # http://www.pydocstyle.org/en/2.1.1/error_codes.html
    ignore = [
        # D200: One-line docstring should fit on one line with quotes
        "D200",
        # D212: Multi-line docstring summary should start at the first line
        "D212",
        # D203: 1 blank line required before class docstring (found 0)
        "D203",
        # D205: 1 blank line required between summary line and description
        "D205",
        # D400: First line should end with a period (not ':')
        "D400",
    ]
    if not pedantic:
        ignore.extend([
            # D100: Missing docstring in public module
            "D100",
            # D102: Missing docstring in public method
            "D102",
            # D103: Missing docstring in public function
            "D103",
            # D107: Missing docstring in __init__
            "D107",
            # D401: First line should be in imperative mood
            "D401",
        ])
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
    res = []
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
            res.append(line)
    #
    return res


def _pyment(file_name, pedantic, check_if_possible):
    _ = pedantic
    executable = "pyment"
    if check_if_possible:
        return _check_exec(executable)
    dbg.dassert_ne(file_name, "")
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
    dbg.dassert_ne(file_name, "")
    if not is_py_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    opts = ""
    ignore = [
        # [C0304(missing-final-newline), ] Final newline missing [pylint]
        "C0304",
        # C0412(ungrouped-imports), ] Imports from package sklearn are not grouped
        "C0412",
        # R1705(no-else-return), ] Unnecessary "elif" after "return"
        "R1705",
    ]
    if not pedantic:
        ignore.extend([
            # [W0125(using-constant-test), ] Using a conditional statement with a
            #   constant value
            "W0125",
            # [W0511(fixme), ]
            "W0511",
            # [W0603(global-statement), ] Using the global statement
            "W0603",
            # [C0111(missing-docstring), ] Missing module docstring
            "C0111",
            # [R0903(too-few-public-methods), ] Too few public methods
            "R0903",
            # [R0912(too-many-branches), ] Too many branches
            "R0912",
            # [R0913(too-many-arguments), ] Too many arguments
            "R0913",
            # R0914(too-many-locals) Too many local variables
            "R0914",
            # [R0915(too-many-statements), ] Too many statements
            "R0915",
        ])
    if ignore:
        opts += "--disable " + ",".join(ignore)
    # Allow short variables, as long as camel-case.
    opts += ' --variable-rgx="[a-z0-9_]{1,30}$"'
    opts += ' --argument-rgx="[a-z0-9_]{1,30}$"'
    opts += " --ignored-modules=pandas --output-format=parseable"
    opts += " -j 4"
    cmd = executable + " %s %s" % (opts, file_name)
    return _tee(cmd, executable, abort_on_error=False)


def _ipynb_format(file_name, pedantic, check_if_possible):
    _ = pedantic
    curr_path = os.path.dirname(os.path.realpath(__file__))
    executable = '%s/ipynb_format.py' % curr_path
    if check_if_possible:
        return _check_exec(executable)
    dbg.dassert_ne(file_name, "")
    if not is_ipynb_file(file_name):
        _LOG.debug("Skipping file_name='%s'", file_name)
        return []
    cmd = executable + " %s" % file_name
    _system(cmd)
    return []


# ##############################################################################

# There are 3 possible ways to handle notebooks.
# 1) Skip linting py/ipynb files
# 2) ignore paired py file, run run_ipynb_format() on ipynb, and then update py
#    file with jupytext
# 3) Run linter on py file, and then update ipynb with jupytext
#   - This is a bit risky for now


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


def toggle_jupytext_file(file_name):
    """
    Get the paired jupytext file name.
    """
    if is_py_file(file_name):
        ret = file_name.replace(".py", ".ipynb")
    elif is_ipynb_file(file_name):
        ret = file_name.replace(".ipynb", ".py")
    else:
        raise ValueError("Not a valid jupytext filename='%s'" % file_name)
    return ret


def is_paired_jupytext_file(file_name):
    """
    Return whether a file is a paired jupytext file.
    """
    is_paired = ((is_py_file(file_name) and
                  os.path.exists(toggle_jupytext_file(file_name)) or
                  (is_ipynb_file(file_name) and
                   os.path.exists(toggle_jupytext_file(file_name)))))
    return is_paired


class JupytextProcessor:
    """
    - If there is a ipynb but no corresponding py then generate the py file
      - Issue a warning about updating with jupytext
    - TODO(gp): Check that paired notebooks are in sync
      - If not, we break since user should decide what to do
    """

    def __init__(self, ipynb_file_name, fix_issues):
        dbg.dassert_isinstance(ipynb_file_name, str)
        dbg.dassert(is_ipynb_file(ipynb_file_name))
        dbg.dassert_exists(ipynb_file_name)
        self.ipynb_file_name = ipynb_file_name
        self.py_file_name = toggle_jupytext_file(ipynb_file_name)
        self.fix_issues = fix_issues

    def process(self):
        output = []
        if is_ipynb_file(self.ipynb_file_name) and not is_paired_jupytext_file(
                self.ipynb_file_name):
            # Missing py file associated to ipynb: need to create py script.
            msg = "Notebook '%s' has no paired jupytext script" % self.ipynb_file_name
            _LOG.warning(msg)
            output.append(msg)
            if self.fix_issues:
                _LOG.warning("Fixing by creating py file '%s'",
                             self.py_file_name)
                cmd = "jupytext --to py:percent %s" % self.ipynb_file_name
                _system(cmd)
                # Add to git.
                dbg.dassert_exists(self.py_file_name)
                cmd = "git add %s" % self.ipynb_file_name
                _system(cmd)
        if is_paired_jupytext_file(self.ipynb_file_name):
            # Both py and ipynb files exist.
            # Remove empty spaces.
            _clean_file(self.ipynb_file_name, write_back=True)
            _clean_file(self.py_file_name, write_back=True)
            # Check that they are consistent. Assume that the ipynb is the
            # correct one.
            # TODO(gp): We should compare timestamp?
            src_py_name = self.py_file_name
            dst_py_name = os.path.join(_TMP_DIR, src_py_name)
            io_.create_enclosing_dir(dst_py_name, incremental=True)
            cmd = "jupytext --to py:percent %s -o %s" % (src_py_name,
                                                         dst_py_name)
            _system(cmd)
            _clean_file(dst_py_name, write_back=True)
            cmd = "diff --ignore-blank-lines %s %s" % (src_py_name, dst_py_name)
            rc = _system(cmd, abort_on_error=False)
            if rc != 0:
                msg = "py file for %s is different: diff with" % src_py_name
                msg += " vimdiff %s %s" % (src_py_name, dst_py_name)
                _LOG.warning(msg)
                output.append(msg)
        return output


def _jupytext_helper(file_name, pedantic, check_if_possible, fix_issues):
    _ = pedantic
    executable = "jupytext"
    if check_if_possible:
        return _check_exec(executable)
    #
    if is_ipynb_file(file_name):
        p = JupytextProcessor(file_name, fix_issues)
        output = p.process()
    else:
        output = []
    return output


def _check_jupytext(file_name, pedantic, check_if_possible):
    fix_issues = False
    output = _jupytext_helper(file_name, pedantic, check_if_possible,
                              fix_issues)
    return output


def _fix_jupytext(file_name, pedantic, check_if_possible):
    fix_issues = True
    output = _jupytext_helper(file_name, pedantic, check_if_possible,
                              fix_issues)
    return output


# #############################################################################
# Main.
# #############################################################################

# To ensure determinism we execute:
# - actions that write first in order (in order of aggressiveness)
# - actions that read only in parallel
_VALID_ACTIONS_META = [
    ("basic_hygiene", "w"),
    ("yapf", "w"),
    ("autoflake", "w"),
    ("isort", "w"),
    ("pyment", "w"),
    ("fix_jupytext", "w"),
    ("ipynb_format", "w"),
    #
    ("pylint", "r"),
    ("check_jupytext", "r"),
    ("flake8", "r"),
    ("pydocstyle", "r"),
]

_VALID_ACTIONS = list(zip(*_VALID_ACTIONS_META))[0]

_ALL_ACTIONS = [
    "basic_hygiene",
    "autoflake",
    "isort",
    "yapf",
    # Disabled because of "import configparser" error.
    #"flake8",
    "pydocstyle",
    "pylint",
    "ipynb_format",
    "fix_jupytext",
]


def _main(args):
    dbg.init_logger(args.log_level)
    #
    if args.test_actions:
        _test_actions()
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    # Select files.
    file_names = _get_files(args)
    _LOG.info("# Processing %s files:\n%s", len(file_names),
              print_.space("\n".join(file_names)))
    # Select actions.
    if args.collect_only:
        _LOG.warning("Exiting as requested")
        sys.exit(0)
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
        if action not in _VALID_ACTIONS:
            raise ValueError("Invalid action '%s'" % action)
    # Reorder actions according to _VALID_ACTIONS.
    actions_tmp = []
    for action in _VALID_ACTIONS:
        if action in actions:
            actions_tmp.append(action)
    # Check that tools are available.
    actions = _remove_not_possible_actions(actions)
    actions_as_str = [
        "%16s: %s" % (a, "Yes" if a in actions else "-") for a in _VALID_ACTIONS
    ]
    _LOG.info("# Action selected:\n%s", print_.space("\n".join(actions_as_str)))
    # Create tmp dir.
    io_.create_dir(_TMP_DIR, incremental=False)
    # Run linter.
    output = []
    num_steps = len(file_names) * len(actions)
    _LOG.info("Num of files=%d, num of actions=%d -> num of steps=%d",
              len(file_names), len(actions), num_steps)
    if args.progress_bar:
        progress_bar = tqdm.tqdm(total=num_steps)
    else:
        progress_bar = None
    for file_name in file_names:
        if not progress_bar:
            _LOG.info("\n%s", print_.frame(file_name, char1="/"))
        for action in actions:
            if not progress_bar:
                _LOG.debug("\n%s", print_.frame(action, char1="-"))
                print("## " + action)
            if args.debug:
                dst_file_name = file_name + "." + action
                cmd = "cp -a %s %s" % (file_name, dst_file_name)
                os.system(cmd)
            else:
                dst_file_name = file_name
            func = _get_action_func(action)
            pedantic = not args.no_pedantic
            # We want to run the stages, and not check.
            check_if_possible = False
            output_tmp = func(dst_file_name, pedantic, check_if_possible)
            # Annotate with executable [tag].
            output_tmp = _annotate(output_tmp, action)
            dbg.dassert_isinstance(
                output_tmp,
                list,
                msg="action=%s file_name=%s" % (action, file_name))
            output.extend(output_tmp)
            if progress_bar:
                progress_bar.update()
            else:
                if output_tmp:
                    _LOG.info("\n%s", "\n".join(output_tmp))
    output.append("cmd line='%s'" % _get_command_line())
    # TODO(gp): Get timestamp.
    output.append("datetime='%s'" % datetime.datetime.now())
    output = _remove_empty_lines(output)
    # Print linter output.
    print(print_.frame(args.linter_log, char1="/").rstrip("\n"))
    print("\n".join(output) + "\n")
    print(print_.line().rstrip("\n"))
    # Write file.
    f = open(args.linter_log, "w")
    output = "\n".join(output)
    f.write(output)
    f.close()
    # Compute the number of lints.
    num_lints = 0
    for line in output.split("\n"):
        # dev_scripts/linter.py:493: ... [pydocstyle]
        if re.search(r"\S+:\d+.*\[\S+\]", line):
            num_lints += 1
    _LOG.info("num_lints=%d", num_lints)
    if num_lints != 0:
        _LOG.info("You can quickfix the issues with\n> vim -c 'cfile %s'",
                  args.linter_log)
    #
    if not args.no_cleanup:
        io_.delete_dir(_TMP_DIR)
    else:
        _LOG.warning("Leaving tmp files in '%s'", _TMP_DIR)
    return num_lints


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Select files.
    parser.add_argument(
        "-f", "--files", nargs='+', type=str, help="Files to process")
    parser.add_argument(
        '-c',
        '--current_git_files',
        action="store_true",
        help="Select all files modified in the current git client")
    parser.add_argument(
        '-p',
        '--previous_git_commit_files',
        nargs="?",
        type=int,
        const=1,
        default=None,
        help="Select all files modified in previous 'n' user git commit")
    parser.add_argument(
        '-d',
        '--dir_name',
        action="store",
        help="Select all files in a dir. 'GIT_ROOT' to select git root")
    # Select files based on type.
    parser.add_argument(
        "--skip_py", action="store_true", help="Do not process python scripts")
    parser.add_argument(
        "--skip_ipynb",
        action="store_true",
        help="Do not process jupyter notebooks")
    parser.add_argument(
        "--skip_jupytext",
        action="store_true",
        help="Do not process paired notebooks")
    parser.add_argument(
        "--only_py", action="store_true", help="Process only python scripts")
    parser.add_argument(
        "--only_ipynb",
        action="store_true",
        help="Process only jupyter notebooks")
    parser.add_argument(
        "--only_jupytext",
        action="store_true",
        help="Process only paired notebooks")
    # Debug.
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Generate one file per transformation")
    parser.add_argument(
        "--no_cleanup", action="store_true", help="Do not clean up tmp files")
    parser.add_argument(
        "--progress_bar",
        action="store_true",
        help="Use progress bar instead of verbose output")
    # Test.
    parser.add_argument(
        "--collect_only",
        action="store_true",
        help="Print only the files to process and stop")
    parser.add_argument(
        "--test_actions",
        action="store_true",
        help="Print the possible actions")
    # Select actions.
    parser.add_argument(
        "--action", action="append", help="Run a specific check")
    parser.add_argument(
        "--quick", action="store_true", help="Run all quick phases")
    parser.add_argument(
        "--all", action="store_true", help="Run all recommended phases")
    parser.add_argument(
        "--no_pedantic", action="store_true", help="Skip purely cosmetic lints")
    #
    parser.add_argument(
        "--linter_log",
        default="./linter_warnings.txt",
        help="File storing the warnings")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()
    rc = _main(args)
    sys.exit(rc)


if __name__ == '__main__':
    _parse()