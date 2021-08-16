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


def _test1() -> None:
    """
    Call the intrinsic function.
    """
    _ = _func()


def _test2() -> None:
    """
    """
    tag = "manage_cache"
    hcac.clear_global_cache("all", tag=tag)
    # Create a function-specific cache on disk only.
    path = "/tmp/cache.function"
    _func.set_cache_path("disk", path)
    _func.clear_cache(cache_type="disk")
    #
    _func()
    print(_func.get_info())
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
        hcac.clear_global_cache("all", tag=tag)
    elif action == "clear_mem_cache":
        hcac.clear_global_cache("mem", tag=tag)
    elif action == "clear_disk_cache":
        hcac.clear_global_cache("disk", tag=tag)
    elif action == "print_cache_info":
        txt = hcac.get_global_cache_info()
        print(txt)
    elif action == "test":
        _test2()
    elif action == "list":
        print("Valid actions are:\n%s" % "\n".join(actions))
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
