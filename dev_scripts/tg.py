#!/usr/bin/env python

"""
Send a notification through Telegram. See README.md in helpers/telegram_notify.

> cmd_to_check; tntnot.py -m "error=$?"

> ls; tntnot.py -m "error=$?"
> ls /I_do_not_exist; tntnot.py -m "error=$?"

Import as:

import dev_scripts.tg as dstg
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as hparse
import helpers.system_interaction as hsinte
import helpers.telegram_notify.telegram_notify as tntnot

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-m", "--msg", action="store", default="done", type=str)
    parser.add_argument(
        "--cmd",
        action="store",
        default=None,
        type=str,
        help="Command to execute",
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    message = []
    message.append("user=%s" % hsinte.get_user_name())
    message.append("server=%s" % hsinte.get_server_name())
    #
    if args.cmd is not None:
        cmd = args.cmd
        _LOG.info("Executing: %s", cmd)
        rc = hsinte.system(cmd, suppress_output=False, abort_on_error=False)
        _LOG.info("rc=%s", rc)
        message.append("cmd='%s'" % cmd)
        message.append("rc=%s" % rc)
    else:
        message.append("msg=%s" % args.msg)
    message = "\n" + "\n".join(message)
    _LOG.info(message)
    #
    tgn = tntnot.TelegramNotify()
    tgn.send(message)


if __name__ == "__main__":
    _main(_parse())
