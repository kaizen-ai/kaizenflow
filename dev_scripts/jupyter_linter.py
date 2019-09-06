#!/usr/bin/env python
"""
Reformat and lint python and ipynb files.

"""

import argparse
import datetime
import itertools
import logging
import os
import re
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as printing
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

# There are 3 possible ways to handle notebooks.
# 1) Skip linting py/ipynb files
# 2) ignore paired py file, run run_ipynb_format() on ipynb, and then update py
#    file with jupytext
# 3) Run linter on py file, and then update ipynb with jupytext
#   - This is a bit risky for now

# Lint all py files that are not paired.
# Lint all paired py files and run jupytest --update.
# Lint all ipynb files and run jupytext.


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


class JupytextProcessor:
    """
    - If there is a ipynb but no corresponding py then generate the py file
      - Issue a warning about updating with jupytext
    - TODO(gp): Check that paired notebooks are in sync
      - If not, we break since user should decide what to do
    """

    def __init__(self, ipynb_file_name, fix_issues):
        dbg.dassert(is_ipynb_file(ipynb_file_name))
        dbg.dassert_exists(ipynb_file_name)
        #
        self.ipynb_file_name = ipynb_file_name
        self.py_file_name = from_ipynb_to_python_file(ipynb_file_name)
        self.fix_issues = fix_issues

    def process(self):
        output = []
        if is_ipynb_file(self.ipynb_file_name) and (
            not is_paired_jupytext_file(self.ipynb_file_name)
        ):
            # Missing py file associated to ipynb: need to create py script.
            msg = (
                "Notebook '%s' has no paired jupytext script"
                % self.ipynb_file_name
            )
            _LOG.warning(msg)
            output.append(msg)
            if self.fix_issues:
                _LOG.warning("Fixing by creating py file '%s'", self.py_file_name)
                cmd = "jupytext --to py:percent %s" % self.ipynb_file_name
                _system(cmd)
                # Add to git.
                dbg.dassert_exists(self.py_file_name)
                cmd = "git add %s" % self.ipynb_file_name
                _system(cmd)
        #
        if is_paired_jupytext_file(self.ipynb_file_name):
            # Both py and ipynb files exist.
            # Remove empty spaces.
            # _clean_file(self.ipynb_file_name, write_back=True)
            # _clean_file(self.py_file_name, write_back=True)
            # Check that they are consistent. Assume that the ipynb is the
            # correct one.
            # TODO(gp): We should compare timestamp?
            src_py_name = self.py_file_name
            dst_py_name = os.path.join(_TMP_DIR, src_py_name)
            dir_name = io_.create_enclosing_dir(dst_py_name, incremental=True)
            _LOG.debug("Created dir_name '%s'", dir_name)
            dbg.dassert_exists(dir_name)
            cmd = "jupytext --to py:percent %s -o %s" % (src_py_name, dst_py_name)
            _system(cmd)
            # _clean_file(dst_py_name, write_back=True)
            cmd = "diff --ignore-blank-lines %s %s" % (src_py_name, dst_py_name)
            rc = _system(cmd, abort_on_error=False)
            if rc != 0:
                if self.fix_issues:
                    # Check the timestamps
                    # If the .py file has a newer timestamp don't do anything.
                    # If the .ipynb is newer, update the .py file, call the linter.
                    pass
                else:
                    msg = (
                        "py file for '%s' is different: diff with:" % src_py_name
                    )
                    msg += " vimdiff %s %s" % (src_py_name, dst_py_name)
                    _LOG.warning(msg)
                    output.append(msg)
            # TODO(gp):
            # Lint the .py file.
            # Re-apply it to the ipynb.
            # Maybe it's best to have an executable to this work.
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
    output = _jupytext_helper(file_name, pedantic, check_if_possible, fix_issues)
    return output


def _fix_jupytext(file_name, pedantic, check_if_possible):
    fix_issues = True
    output = _jupytext_helper(file_name, pedantic, check_if_possible, fix_issues)
    return output


def _main(args):
    dbg.init_logger(args.log_level)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Select files.
    parser.add_argument(
        "-f", "--files", nargs="+", type=str, help="Files to process"
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
    # Select actions.
    parser.add_argument("--action", action="append", help="Run a specific check")
    #
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()
    _main(args)


if __name__ == "__main__":
    _parse()
