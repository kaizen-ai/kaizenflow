#!/usr/bin/env python

import argparse
import logging

import helpers.dbg as dbg
import helpers.env as env
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_verbosity_arg(parser)
    parser.add_argument(
        "--conda_env_name", help="Environment name", type=str, required=True,
    )
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    msg, file_name = env.save_env_file(args.conda_env_name, dir_name=None)
    print("file_name=%s", file_name)
    print(msg)


if __name__ == "__main__":
    _main()
