#!/usr/bin/env python

"""
Implement several Git workflows on multiple repos.

# Show the current state of the submodules.
> dev_scripts/git/git_submodules.py
"""

import argparse
import logging
from typing import List

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _pull(dir_names: List[str]) -> None:
    _LOG.info("status=\n%s", git.report_submodule_status(dir_names))
    for dir_name in dir_names:
        cmd = f"cd {dir_name} && git pull --autostash"
        si.system(cmd)
    _LOG.info("status=\n%s", git.report_submodule_status(dir_names))


def _show(dir_names: List[str]) -> None:
    print(git.report_submodule_status(dir_names))


def _roll_fwd(dir_names: List[str]) -> None:
    _LOG.info("status=\n%s", git.report_submodule_status(dir_names))
    for dir_name in dir_names:
        cmd = f"cd {dir_name} && git pull --autostash"
        si.system(cmd)
        cmd = f"git add {dir_name}"
        si.system(cmd)
    _LOG.info("status=\n%s", git.report_submodule_status(dir_names))
    cmd = 'git commit -am "Move fwd amp and infra" && git push'
    script_name = "./tmp_push.sh"
    si.create_executable_script(script_name, cmd)
    msg: List[str] = []
    msg.append("Run:")
    msg.append(f"> {cmd}")
    msg.append("or")
    msg.append(f"> {script_name}")
    msg_as_str = "\n".join(msg)
    print(msg_as_str)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--pull", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--roll_fwd", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=False)
    #
    dir_names = git.get_repo_dirs()
    _LOG.info("dir_names=%s", dir_names)
    if args.show:
        _show(dir_names)
    if args.roll_fwd:
        _roll_fwd(dir_names)


if __name__ == "__main__":
    _main(_parse())
