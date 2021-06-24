#!/usr/bin/env python
import argparse

import helpers.cache as hcac
import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as hsyste


@hcac.cache(set_verbose_mode=True)
def _test() -> str:
    txt = "#" * 1024 ** 2
    return txt


def _warm_up_cache():
    _ = _test()


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
    cache_types = hcac.get_cache_types()
    tag = None
    actions = [
        "clear_cache",
        "clear_mem_cache",
        "clear_disk_cache",
        "list",
        "print_cache_info",
        "warm_up"]
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
            cache_info = hcac.get_cache_size_info(cache_type, tag=tag)
            print(cache_info)
    elif action == "warm_up":
        _warm_up_cache()
    elif action == "list":
        print("Valid actions are:\n%s" % "\n".join(actions))
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())