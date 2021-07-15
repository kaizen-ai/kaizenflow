#!/usr/bin/env python

"""
Convert a URL or a path into different formats, e.g., Jupyter URL, GitHub, Git path.

> url.py https://github.com/.../.../Task229_Exploratory_analysis_of_ST_data.ipynb
file_name=
/Users/saggese/src/.../.../oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb

github_url=
https://github.com/.../.../Task229_Exploratory_analysis_of_ST_data.ipynb

jupyter_url=
http://localhost:10001/tree/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
"""

import argparse
import logging
import os
import re
import sys
from typing import Tuple

import requests

import helpers.dbg as dbg
import helpers.network as hnetwor
import helpers.git as git
import helpers.parser as hparse
import helpers.printing as hprint
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)


def _print(tag: str, val: str, verbose: bool) -> None:
    if verbose:
        print("\n# %s\n%s" % (hprint.color_highlight(tag, "green"), val))
    else:
        print("\n" + val)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*")
    parser.add_argument("--short", action="store_true", help="Short output form")
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, force_print_format=True)
    #
    positional = args.positional
    if len(positional) != 1:
        print("Need to specify one 'url'")
        sys.exit(-1)
    #
    verbosity = not args.short
    github_prefix, jupyter_prefix = hnetwor.get_prefixes()
    _print("github_prefix", github_prefix, verbosity)
    _print("jupyter_prefix", jupyter_prefix, verbosity)
    #
    url = positional[0]
    rel_file_name = hnetwor.get_file_name(url)
    _print("rel_file_name", rel_file_name, verbosity)
    if not rel_file_name:
        msg = "Can't extract the name of a file from '%s'" % url
        raise ValueError(msg)
    #
    _print("file_name", rel_file_name, verbosity)
    #
    abs_file_name = git.get_client_root(super_module=True) + "/" + rel_file_name
    _print("abs file_name", abs_file_name, verbosity)
    #
    github_url = github_prefix + "/" + rel_file_name
    _print("github_url", github_url, verbosity)
    #
    jupyter_url = jupyter_prefix + "/" + rel_file_name
    _print("jupyter_url", jupyter_url, verbosity)
    #
    if rel_file_name.endswith(".ipynb"):
        cmd = "publish_notebook.py --file %s --action open" % abs_file_name
        _print("read notebook", cmd, verbosity)

    #
    print()
    if not os.path.exists(abs_file_name):
        _LOG.warning("'%s' doesn't exist", abs_file_name)
    hnetwor.check_url(github_url)
    hnetwor.check_url(jupyter_url)


if __name__ == "__main__":
    _main(_parse())
