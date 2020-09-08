#!/usr/bin/env python

"""# Download adjustments from kibot since 5/1/2020.

> download_adjustments.py   \
    -u kibot_username       \
    -p kibot_password       \
    -s 5/1/2020
"""
import argparse
import logging
import os
import sys

import requests

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.s3 as hs3
import helpers.system_interaction as si
import vendors2.kibot.metadata.config as config

_LOG = logging.getLogger(__name__)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-u", "--username", required=True, help="Specify username",
    )
    parser.add_argument(
        "-p", "--password", required=True, help="Specify password",
    )
    parser.add_argument(
        "-s",
        "--start_date",
        required=True,
        help="Start date to download adjustments since, i.e '5/1/2020'",
    )
    parser.add_argument(
        "--tmp_dir",
        type=str,
        nargs="?",
        help="Directory to store temporary data",
        default="tmp.kibot_downloader",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Clean the local directories",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> int:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create dirs.
    incremental = not args.no_incremental
    io_.create_dir(args.tmp_dir, incremental=incremental)

    # Log in to API.
    response = requests.get(
        url=config.API_ENDPOINT,
        params=dict(action="login", user=args.username, password=args.password,),
    )
    status_code = int(response.text.split()[0])
    accepted_status_codes = [
        200,  # login successfuly
        407,  # user already logged in
    ]
    dbg.dassert_in(
        status_code,
        accepted_status_codes,
        msg=f"Failed to login: {response.text}",
    )

    # TODO(amr): confirm last available start date.

    # Download file.
    response = requests.get(
        url=config.API_ENDPOINT,
        params=dict(
            action="adjustments", symbol="allsymbols", startdate=args.start_date,
        ),
    )

    file_path = os.path.join(
        args.tmp_dir, config.ADJUSTMENTS_SUB_DIR, config.ADJUSTMENTS_FILE_NAME
    )
    io_.to_file(file_name=file_path, lines=str(response.content, "utf-8"))
    _LOG.info("Downloaded file to: %s", file_path)

    # Save to s3.
    aws_path = os.path.join(
        config.S3_PREFIX, config.ADJUSTMENTS_SUB_DIR, config.ADJUSTMENTS_FILE_NAME
    )
    hs3.check_valid_s3_path(aws_path)
    # TODO(amr): create hs3.copy() helper.
    cmd = "aws s3 cp %s %s" % (file_path, aws_path)
    si.system(cmd)
    _LOG.info("Uploaded file to s3: %s", aws_path)

    return 0


if __name__ == "__main__":
    sys.exit(_main(_parse()))
