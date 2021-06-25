#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.

Check dev_scripts/linter.py to see an example of a script using this
template.
"""

import argparse
import logging
import pprint
import os
import sys
import time
from typing import Any, Callable, Dict, List, Tuple

import joblib
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.parser as prsr
import helpers.joblib_helpers as hjoblib

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _func(val1: int, val2: str,
          #
          incremental: bool, abort_on_error: bool,
          #
          **kwargs: Any,
          ) -> str:
    res = f"val1={val1} val2={val2} kwargs={kwargs} incremental={incremental} abort_on_error={abort_on_error}"
    _LOG.debug("res=%s", res)
    time.sleep(1)
    if val1 == -1:
        if abort_on_error:
            raise ValueError(f"Error: {res}")
    return res


def _get_workload() -> hjoblib.WORKLOAD:
    tasks = []
    for _ in range(5):
        # val1, val2
        task = ((5, 6), {"hello": "world", "good": "bye"})
        tasks.append(task)
    #task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    #tasks.append(task)
    return (_func, tasks)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    parser.add_argument(
        "--clean_dst_dir",
        action="store_true",
        help="Delete the destination dir before running workload",
    )
    prsr.add_parallel_processing_arg(parser)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Prepare the workload.
    func, tasks = _get_workload()
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    hio.create_dir(dst_dir, incremental=not args.clean_dst_dir)
    # Parse command-line options.
    dry_run = args.dry_run
    num_threads = args.num_threads
    incremental = not args.no_incremental
    abort_on_error = not args.skip_on_error
    # Execute.
    res = hjoblib.parallel_execute(func, tasks, dry_run, num_threads, incremental, abort_on_error)
    _ = res


if __name__ == "__main__":
    _main(_parse())
