#!/usr/bin/env python

import argparse
import logging
import concurrent.futures
import asyncio
import os

import helpers.hdbg as hdbg
import helpers.hparser as hparser


_LOG = logging.getLogger(__name__)

# #############################################################################

def list_files():
    return os.listdir("/app/optimizer/tmp_dir")


def on_got_files(fut):
   print("got files {}".format(fut.result()))
   loop.stop()


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    with concurrent.futures.ProcessPoolExecutor() as executor:
       loop = asyncio.get_event_loop()
       fut = loop.run_in_executor(executor, list_files)
       fut.add_done_callback(on_got_files)
       print("Listing files asynchronously")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.call_soon(_main(_parse()))
    loop.run_forever()