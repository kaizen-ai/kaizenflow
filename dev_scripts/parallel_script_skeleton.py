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
from typing import Any, Dict, List, Tuple

import joblib
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.parser as prsr

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


# #############################################################################


def _decorate_kwargs(kwargs: Dict, incremental: bool, abort_on_error: bool) -> Dict:
    kwargs = kwargs.copy()
    kwargs.update({
        "incremental": incremental,
        "abort_on_error": abort_on_error})
    return kwargs


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
    # Create the dst dir.
    dst_dir = os.path.abspath(args.dst_dir)
    hio.create_dir(dst_dir, incremental=not args.clean_dst_dir)
    # Parse command-line options.
    abort_on_error = not args.skip_on_error
    incremental = not args.no_incremental
    num_threads = args.num_threads
    _LOG.info("num_threads=%s", num_threads)
    # Prepare the workload.
    func = _func
    tasks = []
    for _ in range(5):
        # val1, val2
        task = ((5, 6), {"hello": "world", "good": "bye"})
        tasks.append(task)
    #task = ((-1, 7), {"hello2": "world2", "good2": "bye2"})
    #tasks.append(task)
    if args.dry_run:
        for i, task in tqdm(enumerate(tasks)):
            print("\n" + hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            print(pprint.pformat(task))
        _LOG.warning("Exiting without executing, as per user request")
        sys.exit(0)
    # Run.
    if num_threads == "serial":
        res = []
        for i, task in tqdm(enumerate(tasks)):
            _LOG.debug("\n%s", hprint.frame("Task %s / %s" % (i + 1, len(tasks))))
            #
            res_tmp = func(
                *task[0],
                **_decorate_kwargs(task[1], incremental, abort_on_error)
            )
            res.append(res_tmp)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info("Using %d threads", num_threads)
        res = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(func)(
                *task[0],
                **_decorate_kwargs(task[1], incremental, abort_on_error)
            )
            for task in tasks
        )
    _ = res


if __name__ == "__main__":
    _main(_parse())
