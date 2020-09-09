#!/usr/bin/env python

"""# Make an API call every 10 seconds to get the history of symbol `MSFT`

> check_realtime_feed.py -u $P1_KIBOT_USERNAME -p $P1_KIBOT_PASSWORD
"""

import argparse
import logging
import sys
import time

import requests

import helpers.dbg as dbg
import helpers.parser as prsr
import vendors2.kibot.data.config as config

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
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> int:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)

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

    # Download file.
    while True:
        response = requests.get(
            url=config.API_ENDPOINT,
            params=dict(
                action="history", symbol="MSFT", interval="1", period="2"
            ),
        )

        print(f"received {len(response.text.split())} data points.")
        time.sleep(10)
    return 0


if __name__ == "__main__":
    sys.exit(_main(_parse()))
