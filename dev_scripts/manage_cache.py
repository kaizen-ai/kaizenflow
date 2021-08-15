#!/usr/bin/env python
import argparse

import helpers.cache as hcac
import helpers.dbg as dbg
import helpers.parser as prsr

# Example functions for testing.


@hcac.cache(set_verbose_mode=True)
def _func() -> str:
    txt = "#" * 1024 ** 2
    return txt


def _test() -> None:
    _ = _func()


def _test2() -> None:
    tag = None
    hcac.clear_global_cache("mem", tag=tag)
    path = "/tmp/cache.function"
    _func.set_cache_path("disk", path)
    #
    _func.clear_cache(cache_type="disk")
    #
    _func()
    hcac.clear_global_cache("mem", tag=tag)
    #
    _func()


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--action", required=True, type=str)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    action = args.action
    cache_types = hcac._get_cache_types()
    tag = None
    actions = [
        "clear_cache",
        "clear_mem_cache",
        "clear_disk_cache",
        "list",
        "print_cache_info",
        "test",
    ]
    dbg.dassert_in(action, actions)
    if action == "clear_cache":
        print("cache_types=%s" % str(cache_types))
        for cache_type in cache_types:
            hcac.clear_global_cache(cache_type, tag=tag)
    elif action == "clear_mem_cache":
        hcac.clear_global_cache("mem", tag=tag)
    elif action == "clear_disk_cache":
        hcac.clear_global_cache("disk", tag=tag)
    elif action == "print_cache_info":
        print("cache_types=%s" % str(cache_types))
        for cache_type in cache_types:
            path = hcac._get_cache_path(cache_type, tag=tag)
            cache_info = hcac.get_cache_size_info(path, cache_type)
            print(cache_info)
    elif action == "test":
        _test2()
    elif action == "list":
        print("Valid actions are:\n%s" % "\n".join(actions))
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
