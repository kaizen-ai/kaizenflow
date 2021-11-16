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
        "--host",
        action="store",
        type=str,
        help="Postgres host to connect",
    )
    parser.add_argument(
        "--db",
        action="store",
        type=str,
        help="Postgres db to connect",
    )
    parser.add_argument(
        "--port",
        action="store",
        type=str,
        help="Postgres port to connect",
    )
    parser.add_argument(
        "--user",
        action="store",
        type=str,
        help="Postgres user to connect",
    )
    parser.add_argument(
        "--password",
        action="store",
        type=str,
        help="Postgres password to connect",
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
    credentials_input = (
        args.host and args.dbname and args.port and args.user and args.password
    )
    if credentials_input:
        connection, _ = hsql.get_connection(
            host=args.host,
            dbname=args.dbname,
            port=args.port,
            user=args.user,
            password=args.password,
        )
    elif args.db_connection == "from_env":
        connection, _ = hsql.get_connection_from_env_vars()
    # Create db with all tables.
    imcdbcrdb.create_im_database(
        connection=connection, new_db=args.db_name, overwrite=args.overwrite
    )


if __name__ == "__main__":
    _main(_parse())
