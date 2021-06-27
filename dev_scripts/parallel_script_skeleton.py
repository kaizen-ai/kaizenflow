#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.

Check dev_scripts/linter.py to see an example of a script using this
template.

# Run with:
> clear; parallel_script_skeleton.py --num_threads serial --dst_dir dst_dir
"""

import argparse
import logging
import os
import time
from typing import Any

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.joblib_helpers as hjoblib
import helpers.parser as prsr

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _func(
    val1: int,
    val2: str,
    **kwargs: Any,
) -> str:
    incremental = kwargs.pop("incremental")
    res = (
        f"val1={val1} val2={val2} kwargs={kwargs} incremental={incremental}"
    )
    _LOG.debug("res=%s", res)
    time.sleep(0.1)
    if val1 == -1:
        raise ValueError(f"Error: {res}")
    return res


def _get_workload1() -> hjoblib.WORKLOAD:
    """
    Workload without any failing tasks.
    """
    tasks = []
    for _ in range(5):
        # val1, val2
        task = ((5, 6), {"hello": "world", "good": "bye"})
        tasks.append(task)
    return (_func, tasks)


def _get_workload2() -> hjoblib.WORKLOAD:
    """
    Workload including a failing task.
    """
    _func, tasks = _get_workload1()
    # Add a task triggering a failure.
    task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    tasks.append(task)
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
    func, tasks = _get_workload1()
    #func, tasks = _get_workload2()
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    hio.create_dir(dst_dir, incremental=not args.clean_dst_dir)
    # Parse command-line options.
    dry_run = args.dry_run
    num_threads = args.num_threads
    incremental = not args.no_incremental
    abort_on_error = not args.skip_on_error
    log_file = os.path.join(dst_dir, "parallel_execute.log")
    # Execute.
    res = hjoblib.parallel_execute(
        func, tasks, dry_run, num_threads, incremental, abort_on_error,
        log_file,
    )
    _ = res


if __name__ == "__main__":
    _main(_parse())
