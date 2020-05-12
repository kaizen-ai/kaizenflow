#!/usr/bin/env python

"""
# TODO(Sergey): add docs
"""

import argparse
import difflib
import logging
import os
import sys
from typing import List

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si

_log = logging.getLogger(__name__)

ACTION_CHECK_PACKAGES = 'check-packages'
ACTION_RUN_LINTER = 'run-linter'
ACTIONS = [ACTION_CHECK_PACKAGES,
           ACTION_RUN_LINTER, ]


def _print_help(parser):
    print(parser.format_help())
    sys.exit(-1)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-a",
        "--action",
        choices=ACTIONS,
        help=f"Pick action to perform. \n\tActions: {ACTIONS}",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _get_reference_conda_list() -> List[str]:
    reference_data_path = './test_data/conda_list.txt'
    with open(reference_data_path) as f:
        data = f.read()
    return data.split('\n')


def _get_local_conda_list() -> List[str]:
    cmd = 'conda list | sort'
    _, data = si.system_to_string(cmd)
    return data.split('\n')


def _check_packages() -> str:
    # TODO(Sergey): This one still in progress.
    differ = difflib.Differ()
    reference_data = _get_reference_conda_list()
    local_data = _get_local_conda_list()
    diff = differ.compare(reference_data, local_data)
    return '\n'.join(diff)


def _get_modified_files() -> str:
    cmd = 'git status -s | grep " M"'
    _, output = si.system_to_string(cmd, abort_on_error=False)
    return output


def _run_linter_check() -> None:
    modified_files = _get_modified_files()
    dbg.dassert(len(modified_files) == 0,
                msg=f"Commit changes or stash them.\n{modified_files}")
    amp_path = os.environ["AMP"]
    cmd = f"{amp_path}/dev_scripts/linter_master_report.py"
    _, output = si.system_to_string(cmd)
    print(output.strip())


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    if args.action == ACTION_RUN_LINTER:
        _run_linter_check()


if __name__ == "__main__":
    _main(_parse())
