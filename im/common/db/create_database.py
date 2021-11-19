#!/usr/bin/env python
"""
Script to create database using connection.

Use as:

# Create a db named 'test_db' using environment variables:
> im/common/db/create_database.py --db-name 'test_db'

Import as:

import im.common.db.create_database as imcdbcrda
"""

import argparse

import helpers.io_ as hio
import helpers.parser as hparser
import helpers.sql as hsql
import im.common.db.create_db as imcdbcrdb


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db-connection",
        action="store",
        default="from_env",
        type=str,
        help="DB to connect",
    )
    parser.add_argument(
        "--credentials",
        action="store",
        default=None,
        type=str,
        help="Path to json file with details to connect to DB",
    )
    parser.add_argument(
        "--db-name",
        action="store",
        required=True,
        type=str,
        help="DB to create",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="To overwtite existing db",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    if args.credentials:
        connection = hsql.get_connection(**hio.from_json(args.credentials))
    elif args.db_connection == "from_env":
        connection = hsql.get_connection_from_env_vars()
    else:
        connection = hsql.get_connection_from_string(args.db_connection)
    # Create db with all tables.
    imcdbcrdb.create_im_database(
        connection=connection, new_db=args.db_name, overwrite=args.overwrite
    )


if __name__ == "__main__":
    _main(_parse())
