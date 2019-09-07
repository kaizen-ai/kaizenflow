#!/usr/bin/env python
"""
Convert a url / path into different formats: jupyter url, github, git path.
"""

import argparse
import logging
import os
import re

import requests

import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# - Github path:
#   https://github.com/.../.../blob/master/ravenpack/.../Task245_Analyst-ratings.ipynb
# - Jupyter url:
#   http://localhost:9186/notebooks/ravenpack/.../Task245_Analyst-ratings.ipynb
# - Absolute path:
#   /Users/gp/src/git_particleone/ravenpack/.../Task245_Analyst-ratings.ipynb
# - Relative path to the current dir
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Git path relative to root
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb


def _check_url(url):
    request = requests.get(url)
    exists = request.status_code == 200
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
    # "http://localhost:10001/notebooks/oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
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
        # https://github.com/ParticleDev/commodity_research/blob/master/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
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


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-u", "--url", required=True, type=str, action="store")
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
    url = args.url
    file_name = _get_root(url)
    #
    print(file_name)
    if not os.path.exists(file_name):
        _LOG.warning("'%s' doesn't exist")
    #
    github_prefix, jupyter_prefix = _get_prefixes()
    #
    github_url = github_prefix + "/" + file_name
    print(github_url)
    _check_url(github_url)
    #
    jupyter_url = jupyter_prefix + "/" + file_name
    print(jupyter_url)
    _check_url(jupyter_url)


if __name__ == "__main__":
    _main(_parse())
