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

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.joblib_helpers as hjoblib
import helpers.parser as prsr
import helpers.test.test_joblib_helpers

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        default="./tmp.parallel_script_skeleton",
        help="Destination dir",
    )
    parser.add_argument(
        "--clean_dst_dir",
        action="store_true",
        default=True,
        help="Delete the destination dir before running workload",
    )
    prsr.add_parallel_processing_arg(parser)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Prepare the workload.
    randomize = True
    # randomize = False
    seed = 3
    # func, tasks = helpers.test.test_joblib_helpers.get_workload1(randomize, seed=seed)
    func, tasks = helpers.test.test_joblib_helpers.get_workload2(
        randomize, seed=seed
    )
    func_name = "func"
    print(hjoblib.tasks_to_string(func, func_name, tasks))
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
        func,
        func_name,
        tasks,
        dry_run,
        num_threads,
        incremental,
        abort_on_error,
        log_file,
    )
    print("res=\n%s" % "\n".join(map(str, res)))


if __name__ == "__main__":
    _main(_parse())
