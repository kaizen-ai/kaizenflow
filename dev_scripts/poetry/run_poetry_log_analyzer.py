#!/usr/bin/env python
"""
TODO(Nikola): Docs.
"""

import re
import argparse
import logging
import os
from typing import List, Dict, Union

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hgit as hgit

_LOG = logging.getLogger(__name__)


# TODO(Nikola): Remove, already exists.
def get_debug_poetry_dir() -> str:
    """
    Gets working directory of Poetry tool.
    :return: working directory
    """
    amp_path = hgit.get_amp_abs_path()
    poetry_debug_dir = os.path.join(amp_path, "dev_scripts/poetry")
    hdbg.dassert_dir_exists(poetry_debug_dir)
    return poetry_debug_dir


def update_analysis(analysis: Dict[Union[str, Dict]], debug_modes: List[str], time_info: str):
    """
    TODO(Nikola): Docs.
    """
    # TODO(Nikola): Simpler init, default dict?
    if len(debug_modes) > 1:
        if analysis.get(debug_modes[0], None) is None:
            analysis[debug_modes[0]] = {}
        analysis[debug_modes[0]].update({debug_modes[1]: time_info})
    else:
        analysis[debug_modes[0]] = time_info


def check_log_validity(log_file: str):
    """
    TODO(Nikola): Docs.
    :return: execution time information
    """
    time_info = "Not finished"
    locked = re.search(r"Writing lock file", log_file)
    if locked:
        # Get total seconds.
        time_info = re.search(r"Complete version solving took (.*?) seconds", log_file)
        if not time_info:
            # If run is too fast, there will be information only for one iteration.
            time_info = re.search(r"Version solving took (.*?) seconds", log_file)
            if not time_info:
                # New check must be added.
                raise NotImplementedError
        time_info = time_info.group(1)
    return time_info


def run_log_analyzer():
    """
    TODO(Nikola): Docs.
    """
    analysis = {}
    # Collect all logs.
    log_paths = hio.find_regex_files(get_debug_poetry_dir(), "poetry.log")
    for log_path in log_paths:
        # Parse log path to extract debug modes.
        debug_mode_path = log_path.split("poetry" + os.sep)[-1]
        debug_modes = debug_mode_path.split(os.sep)[:-1]
        log_file = hio.from_file(log_path)
        time_info = check_log_validity(log_file)
        update_analysis(analysis, debug_modes, time_info)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    # TODO(Nikola): Args for logs output format?
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    run_log_analyzer()


if __name__ == "__main__":
    _main(_parse())