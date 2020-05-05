#!/usr/bin/env python

"""
# TODO(Sergey): add docs
"""

import argparse
import difflib
import logging
import sys
from typing import List

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si

_log = logging.getLogger(__name__)


def _print_help(parser):
    print(parser.format_help())
    sys.exit(-1)


def _parse() -> argparse.ArgumentParser:
    check_packages_action = 'check-packages'
    actions = [check_packages_action, ]
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-a",
        "--action",
        choices=actions,
        help=f"Pick action to perform. \n\tActions: {actions}",
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
    differ = difflib.Differ()
    reference_data = _get_reference_conda_list()
    local_data = _get_local_conda_list()
    diff = differ.compare(reference_data, local_data)
    return '\n'.join(diff)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)


if __name__ == "__main__":
    _main(_parse())
