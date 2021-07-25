#!/usr/bin/env python

"""
Apply the Git hooks to the entire code base.
"""

import argparse
import logging

import dev_scripts.git.git_hooks.utils as ghutils
import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.printing as hprint
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # parser.add_argument("--text", action="store", type=str, required=True)
    # parser.add_argument("--step", action="store", type=int, required=True)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    _LOG.warning("\n%s", hprint.frame("This is a dry run!"))
    git_root = git.get_client_root(super_module=False)
    cmd = fr'''cd {git_root} && find -type f -name "*" -not -path "*/\.git/*"'''
    _, file_list = hsinte.system_to_string(cmd)
    file_list = file_list.split("\n")
    #
    abort_on_error = False
    # ghutils.check_master()
    # ghutils.check_author()
    ghutils.check_file_size(abort_on_error, file_list=file_list)
    ghutils.check_words(abort_on_error, file_list=file_list)


if __name__ == "__main__":
    _main(_parse())
