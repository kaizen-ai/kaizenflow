#!/usr/bin/env python

import argparse
import logging

import helpers.dbg as dbg
import helpers.env as env

_LOG = logging.getLogger(__name__)


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--conda_env_name", help="Environment name", default="develop", type=str
    )
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    msg, file_name = env.save_env_file(args.conda_env_name, dir_name=None)
    print("file_name=%s", file_name)
    print(msg)


if __name__ == "__main__":
    _main()
