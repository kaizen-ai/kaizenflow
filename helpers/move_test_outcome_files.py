#!/usr/bin/env python

"""
Move test golden outcomes from `test` dirs to `test/<X>` dirs.

> move_test_outcome_files.py <dir> --new_dir_name X
"""

import argparse
import logging
import os
import shutil

import helpers.hdbg as hdbg
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


def _move_outcomes(
    dir_name: str,
    new_dir_name: str,
) -> None:
    """
    Move test outcomes located underneath `dir_name` to `test/<new_dir_name>`.

    :param dir_name: path to the head directory to start from
    :param new_dir_name: name of the folder to move the test outcomes to
    """
    # Get all test dirs underneath `dir_name`.
    test_dirs = []
    for root, _, _ in os.walk(dir_name):
        if root.split("/")[-1] == "test":
            test_dirs.append(root)
    # Get all subdirs within the test dirs.
    test_subdirs = []
    for root in test_dirs:
        test_subdirs.extend([x.path for x in os.scandir(root) if x.is_dir()])
    # Move the test subdirs into `test/<new_dir_name>`.
    for test_subdir in test_subdirs:
        test_subdir_name = test_subdir.split("/")[-1]
        if not test_subdir_name.startswith("Test"):
            # Ignore subdirs that do not contain test inputs/outputs.
            continue
        new_subdir = test_subdir.replace("/test/", f"/test/{new_dir_name}/")
        _LOG.info("Moving %s to %s", test_subdir, new_subdir)
        shutil.move(test_subdir, new_subdir)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("dir", type=str, help="Path to the dir to start from")
    parser.add_argument(
        "--new_dir_name",
        type=str,
        default="outcomes",
        help="The name of the folder to move the test outcomes to",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=False)
    hdbg.dassert_dir_exists(args.dir, f"{args.dir} is not a valid directory")
    _move_outcomes(args.dir, args.new_dir_name)


if __name__ == "__main__":
    _main(_parse())
