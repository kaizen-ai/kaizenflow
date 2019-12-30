#!/usr/bin/env python
"""
Import as:

import helpers.cache as cache

# Use as:
import functools

import helpers.cache as cache


def _read_data(*args, **kwargs):
    _LOG.info("Reading ...")
    ...
    return ...


MEMORY = cache.get_disk_cache()

@MEMORY.cache
def _read_data_from_disk(*args, **kwargs):
    _LOG.info("Reading from disk cache: %s %s", *args, **kwargs)
    data = _read_data(*args, **kwargs)
    return data


@functools.lru_cache(maxsize=None)
def read_data(*args, **kwargs):
    _LOG.info("Reading from mem cache: %s %s", *args, **kwargs)
    data = _read_data_from_disk(*args, **kwargs)
    return data
"""

# #############################################################################

import argparse
import logging
import os

import joblib

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


_MEMORY = None


# TODO(gp): Add cache for unit test.


def get_disk_cache():
    _LOG.debug("get_disk_cache")
    global _MEMORY
    if not _MEMORY:
        file_name = os.path.abspath(
            git.get_client_root(super_module=True) + "/tmp.joblib.cache"
        )
        _MEMORY = joblib.Memory(file_name, verbose=0, compress=1)
    return _MEMORY


def reset_disk_cache():
    get_disk_cache().clear(warn=True)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, choices=["reset_cache"])
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    action = args.positional[0]
    if action == "reset_cache":
        reset_disk_cache()
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
