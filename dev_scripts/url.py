#!/usr/bin/env python

"""
Convert a url / path into different formats: jupyter url, github, git path.

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
import helpers.git as git
import helpers.parser as hparse
import helpers.printing as hprint
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)


# TODO(gp): Move it to a central place, helpers.network?
def check_url(url: str) -> None:
    try:
        request = requests.get(url)
        exists = request.status_code == 200
        # pylint: disable=broad-except
    except Exception:
        # TODO(gp): RuntimeError doesn't seem to catch. Find a narrower
        #  exception to catch.
        exists = False
    if not exists:
        _LOG.warning("url '%s' doesn't exist", url)


def _get_prefixes() -> Tuple[str, str]:
    hsyste.get_user_name()
    jupyter_port = 10001
    _LOG.warning(
        "jupyter_port not defined: using the default one %s", jupyter_port
    )
    repo_name = git.get_repo_full_name_from_client(super_module=False)
    _LOG.debug("repo_name=%s", repo_name)
    github_prefix = "https://github.com/%s/blob/master" % repo_name
    jupyter_prefix = "http://localhost:%s/tree" % jupyter_port
    return github_prefix, jupyter_prefix


def _get_file_name(url: str) -> str:
    """
    Given a url from Jupyter server or github extract the path corresponding to
    the file. E.g.,

    - http://localhost:10001/notebooks/research/...
        oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb
      ->
        oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb

    - https://github.com/.../.../blob/master/...
        oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
      ->
        oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb
    """
    # "http://localhost:10001/notebooks/...
    #   oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
    ret = None
    if ret is None:
        m = re.search(r"http.*://localhost:\d+/(.*)", url)
        if m:
            ret = m.group(1)
            to_remove = "notebooks/"
            idx = ret.index(to_remove)
            if idx >= 0:
                end_idx = idx + len(to_remove)
                ret = ret[end_idx:]
    if ret is None:
        # https://github.com/.../.../blob/master/...
        #   oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
        m = re.search(r"http.*://.*github.com/(.*)", url)
        if m:
            ret = m.group(1)
            # Remove ".../.../blob/master"
            ret = "/".join(ret.split("/")[4:])
    if ret is None:
        if os.path.exists(url):
            ret = url
    if ret is None:
        dbg.dassert_is_not(ret, None, "url=%s", url)
    return ret  # type: ignore


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
    github_prefix, jupyter_prefix = _get_prefixes()
    _print("github_prefix", github_prefix, verbosity)
    _print("jupyter_prefix", jupyter_prefix, verbosity)
    #
    url = positional[0]
    rel_file_name = _get_file_name(url)
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
    check_url(github_url)
    check_url(jupyter_url)


if __name__ == "__main__":
    _main(_parse())
