#!/usr/bin/env python

"""
Send a notification through Telegram. See README.md in helpers/telegram_notify

> cmd_to_check; tg.py -m "error=$?"

> ls; tg.py -m "error=$?"
> ls /I_do_not_exist; tg.py -m "error=$?"
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.telegram_notify.telegram_notify as tg

import helpers.system_interaction as si


_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument("-m", "--msg", action="store", default="done", type=str)
    parser.add_argument("-c", "--command", action="store", default=None, type=str, help="Command to execute")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    if args.command is not None:
        _LOG.info("Executing: %s", args.command)
        assert 0, "Not implemented yet"
    else:
        message = args.msg
    tgn = tg.TelegramNotify()
    tgn.notify(message)


if __name__ == "__main__":
    _main(_parse())