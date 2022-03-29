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

LOOP = asyncio.get_event_loop()


def list_files():
    files = os.listdir("/app/optimizer/tmp_dir")
    print(f"FILES={files}")
    return files


def on_got_files(fut):
    print("got files {}".format(fut.result()))
    LOOP.stop()


def _wait_for_file():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        fut = LOOP.run_in_executor(executor, list_files)
        if fut.result():
            fut.add_done_callback(on_got_files)
            print("Listing files asynchronously")


def wait_for_file():
    LOOP.call_soon(_wait_for_file)
    LOOP.run_forever()


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    wait_for_file()


if __name__ == "__main__":
    _main(_parse())