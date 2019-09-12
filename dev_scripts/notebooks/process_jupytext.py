#!/usr/bin/env python

"""
Automate some common workflows with jupytext.

> find . -name "*.ipynb" | grep -v ipynb_checkpoints | head -3 | xargs -t -L 1 process_jupytext.py --action sync --file

# Pair
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action pair

# Test
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action test
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action test_strict

# Sync
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action sync
"""

import argparse
import logging

import dev_scripts.linter as lin
import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

_EXECUTABLE = "jupytext"


def _pair(file_name):
    if lin.is_paired_jupytext_file(file_name):
        _LOG.warning("The file '%s' seems already paired", file_name)
    # It is a ipynb and it is unpaired: create the python file.
    msg = (
        "There was no paired notebook for '%s': created and added to git"
        % file_name
    )
    _LOG.warning(msg)
    # Convert a notebook into jupytext.
    cmd = []
    cmd.append(_EXECUTABLE)
    cmd.append("--update-metadata")
    cmd.append("""'{"jupytext":{"formats":"ipynb,py:percent"}}'""")
    cmd.append(file_name)
    cmd = " ".join(cmd)
    si.system(cmd)
    # Test the ipynb -> py:percent -> ipynb round trip conversion.
    cmd = _EXECUTABLE + " --test --stop --to py:percent %s" % file_name
    si.system(cmd)
    # Add the .py file.
    cmd = _EXECUTABLE + " --to py:percent %s" % file_name
    si.system(cmd)
    # Add to git.
    py_file_name = lin.from_ipynb_to_python_file(file_name)
    cmd = "git add %s" % py_file_name
    si.system(cmd)


def _sync(file_name):
    if lin.is_paired_jupytext_file(file_name):
        # cmd = _EXECUTABLE + " --sync --update --to py:percent %s" % file_name
        cmd = _EXECUTABLE + " --sync --to py:percent %s" % file_name
        si.system(cmd)
    else:
        _LOG.warning("The file '%s' is not paired: run --pair", file_name)


def _test(file_name, action):
    if action == "test":
        opts = "--test"
    elif action == "test_strict":
        opts = "--test-strict"
    else:
        raise ValueError("Invalid action='%s'" % action)
    cmd = [_EXECUTABLE, opts, "--stop --to py:percent %s" % file_name]
    cmd = " ".join(cmd)
    si.system(cmd)


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-f",
        "--file",
        action="store",
        type=str,
        required=True,
        help="File to process",
    )
    parser.add_argument(
        "--action",
        action="store",
        choices=["pair", "test", "test_strict", "sync"],
        required=True,
        help="Action to perform",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    file_name = args.file
    dbg.dassert_exists(file_name)
    dbg.dassert(
        lin.is_ipynb_file(file_name), "'%s' has no .ipynb extension", file_name
    )
    if args.action == "pair":
        _pair(file_name)
    elif args.action == "sync":
        _sync(file_name)
    elif args.action in ("test", "test_strict"):
        _test(file_name, args.action)
    else:
        raise ValueError("Invalid action '%s'" % args.action)


if __name__ == "__main__":
    _main(_parse())
