#!/usr/bin/env python
import argparse

import helpers.cache as hcac
import helpers.dbg as dbg
import helpers.introspection as hintro
import helpers.parser as prsr
import helpers.system_interaction as hsyste


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--action", required=True, type=str, choices=["info", "reset_cache"])
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    action = args.action
    cache_types = hcac.get_cache_types()
    print("cache_types=%s" % str(cache_types))
    tag = None
    if action == "reset_cache":
        for cache_type in cache_types:
            hcac.reset_cache(cache_type, tag=tag)
    elif action == "info":
        for cache_type in cache_types:
            path = hcac.get_cache_path(cache_type, tag=tag)
            size_in_bytes = hsyste.du(path)
            size_as_str = hintro.get_size_in_bytes(size_in_bytes)
            print("cache_type='%s': size=%s path=%s", cache_type, size_as_str, path)
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())