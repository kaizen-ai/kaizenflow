"""
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
    if args.db_connection == "from_env":
        connection, _ = hsql.get_connection_from_env_vars()
    imcdbcrdb.create_database(
        connection=connection, new_db=args.db_name, overwrite=args.overwrite
    )


if __name__ == "__main__":
    _main(_parse())
