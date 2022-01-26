#!/usr/bin/env python
"""
Script to create IM (Instrument Master) database using the given connection.

# Create a DB named 'test_db' using environment variables:
> im/common/db/create_db.py --db_name 'test_db'

Import as:

import im_v2.common.db.create_db as imvcdcrdb
"""

import argparse

import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.common.db.db_utils as imvcddbut
import im_v2.im_lib_tasks as imvimlita

# TODO(gp): Consider converting create_db and remove_db into invoke tasks.


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db_name",
        action="store",
        required=True,
        type=str,
        help="DB to create",
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        type=str,
        default="local",
        help="Which env is used: local, dev or prod",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="To overwrite existing DB",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Load DB credentials from env file.
    db_stage = args.db_stage
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    #
    db_connection = hsql.get_connection(*connection_params)
    # Create DB with all tables.
    imvcddbut.create_im_database(
        connection=db_connection, new_db=args.db_name, overwrite=args.overwrite
    )


if __name__ == "__main__":
    _main(_parse())
