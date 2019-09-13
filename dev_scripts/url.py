#!/usr/bin/env python
"""
Convert a url / path into different formats: jupyter url, github, git path.

> url.py https://github.com/ParticleDev/commodity_research/blob/master/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
file_name=
/Users/saggese/src/particle/commodity_research/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb

github_url=
https://github.com/ParticleDev/commodity_research/blob/master/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb

jupyter_url=
http://localhost:10001/tree/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
"""

# TODO(gp): Add
# - Relative path to the current dir
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Git path relative to root
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb


import argparse
import logging
import os
import re
import sys

import requests

import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# TODO(gp): Move it to a central place, helpers.network?
def check_url(url):
    try:
        request = requests.get(url)
        exists = request.status_code == 200
    except Exception:
        # TODO(gp): RuntimeError doesn't seem to catch. Find a narrower
        #  exception to catch.
        exists = False
    if not exists:
        _LOG.warning("url '%s' doesn't exist", url)


def _get_prefixes():
    si.get_user_name()
    # pwd = os.getcwd()
    # TODO(gp): Generalize once everyone has an assigned port merging with
    # infra/ssh_config.py.
    github_prefix = (
        "https://github.com/ParticleDev/commodity_research/blob/master"
    )
    jupyter_prefix = "http://localhost:10001/tree"
    # if user == "gp":
    #     if pwd in (
    #         "/data/gp_wd/src/particle1",
    #         "/Users/gp/src/git_particleone_...1",
    #     ):
    #         jupyter_prefix = "http://localhost:9185/notebooks/"
    #     elif pwd in (
    #         "/data/gp_wd/src/particle2",
    #         "/Users/gp/src/git_particleone_...2",
    #     ):
    #         jupyter_prefix = "http://localhost:9186/notebooks/"
    return github_prefix, jupyter_prefix


def _get_root(url):
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
        # https://github.com/ParticleDev/commodity_research/blob/master/...
        #   oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
        m = re.search(r"http.*://.*github.com/(.*)", url)
        if m:
            ret = m.group(1)
            # Remove "ParticleDev/commodity_research/blob/master"
            ret = "/".join(ret.split("/")[4:])
    if ret is None:
        if os.path.exists(url):
            ret = url
    #
    return ret


# #############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*")
    parser.add_argument("--verbose", action="store_true", help="Long output form")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=False)
    #
    github_prefix, jupyter_prefix = _get_prefixes()
    positional = args.positional
    if len(positional) != 1:
        print("Need to specify one 'url'")
        sys.exit(-1)
    root = _get_root(positional[0])
    #
    if args.verbose:
        print("\n# file_name\n%s" % root)
    else:
        print("\n" + root)
    #
    file_name = git.get_client_root(super_module=True) + "/" + root
    if args.verbose:
        print("\n# abs file_name\n%s" % file_name)
    else:
        print(file_name)
    #
    github_url = github_prefix + "/" + root
    if args.verbose:
        print("\n# github_url\n%s" % github_url)
    else:
        print(github_url)
    #
    jupyter_url = jupyter_prefix + "/" + root
    if args.verbose:
        print("\n# jupyter_url\n%s" % jupyter_url)
    else:
        print(jupyter_url)
    #
    if root.endswith(".ipynb"):
        cmd = "publish_notebook.py --file %s --action open" % file_name
        if args.verbose:
            print("\n# read notebook\n%s" % cmd)
        else:
            print(cmd)

    #
    print()
    if not os.path.exists(file_name):
        _LOG.warning("'%s' doesn't exist", file_name)
    check_url(github_url)
    check_url(jupyter_url)


if __name__ == "__main__":
    _main(_parse())
