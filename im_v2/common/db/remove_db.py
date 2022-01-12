#!/usr/bin/env python
"""
Script to remove IM database using connection.

# Remove a DB named 'test_db' using environment variables:
> remove_db.py --db_name 'test_db'
"""

import argparse
import os

import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.common.db.db_utils as imvcodbut


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--credentials",
        action="store",
        default="from_env",
        type=str,
        help=(
            "Connection string"
            "or path to json file with credentials to DB"
            "or parameters from environment with"
        ),
    )
    parser.add_argument(
        "--db_name",
        action="store",
        required=True,
        type=str,
        help="DB to drop",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    json_exists = os.path.exists(os.path.abspath(args.credentials))
    if args.credentials == "from_env":
        connection = hsql.get_connection_from_env_vars()
    elif json_exists:
        connection = hsql.get_connection(**hio.from_json(args.credentials))
    else:
        connection = hsql.get_connection_from_string(args.credentials)
    hsql.remove_database(connection=connection, dbname=args.db_name)


if __name__ == "__main__":
    _main(_parse())
