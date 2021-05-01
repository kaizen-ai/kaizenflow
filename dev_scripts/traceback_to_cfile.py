#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.

Check dev_scripts/linter.py to see an example of a script using this template.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Insert your code here.
    # - Use _LOG.info(), _LOG.debug() instead of printing.
    # - Use dbg.dassert_*() for assertion.
    # - Use si.system() and si.system_to_string() to issue commands.


if __name__ == "__main__":
    _main(_parse())


Traceback (most recent call last):
File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
act = ltasks._get_gh_issue_title(issue_id, repo)
File "/app/amp/lib_tasks.py", line 1265, in _get_gh_issue_title
task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
if repo_short_name == "amp":
NameError: name 'repo_short_name' is not defined