#!/usr/bin/env python
"""
Convert a url / path into different formats: jupyter url, github, git path.
"""

import argparse
import logging

import requests

import helpers.dbg as dbg
from helpers.system_interaction import system, system_to_string

_log = logging.getLogger(__name__)

# - Github path:
#   https://github.com/.../.../blob/master/ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Jupyter url:
#   http://localhost:9186/notebooks/ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Absolute path:
#   /Users/gp/src/git_particleone/ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Relative path to the current dir
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb
# - Git path relative to root
#   ./ravenpack/RP_data_exploration/Task245_Analyst-ratings.ipynb



def _check_url(ret):
    request = requests.get(ret)
    exists = request.status_code == 200
    if not exists:
        _log.warning("url '%s' doesn't exist" % ret)


def _main(args):
    github_prefix = "https://github.com/.../.../blob/master/"
    _, user = system_to_string('whoami')
    _, pwd = system_to_string('pwd')
    # TODO(gp): Generalize once everyone has an assigned port merging with infra/ssh_config.py.
    jupyter_prefix = None
    if user == "gp":
        if pwd in ("/data/gp_wd/src/particle1",
                   "/Users/gp/src/git_particleone_...1"):
            jupyter_prefix = "http://localhost:9185/notebooks/"
        elif pwd in ("/data/gp_wd/src/particle2",
                     "/Users/gp/src/git_particleone_...2"):
            jupyter_prefix = "http://localhost:9186/notebooks/"
    if jupyter_prefix is None:
        raise RuntimeError(
            "Can't recognize user='%s' and pwd='%s'" % (user, pwd))
    # From.
    url = args.format_from
    if url.startswith(github_prefix):
        ret = url.replace(github_prefix, "")
    elif url.startswith(jupyter_prefix):
        ret = url.replace(jupyter_prefix, "")
    else:
        raise ValueError("Invalid url '%s'" % url)
    # To.
    if args.format_to == "github":
        ret = github_prefix + "/" + ret
        # GitHub needs authentication.
        #_check_url(ret)
    elif args.format_to == "jupyter":
        ret = jupyter_prefix + "/" + ret
        _check_url(ret)
    elif args.format_to == "git" or args.to == "path":
        pass
    else:
        raise ValueError
    print(ret)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--from', dest="format_from", required=True, type=str, action='store')
    parser.add_argument(
        '--to',
        dest="format_to",
        required=False,
        type=str,
        choices=["github", "jupyter", "git", "path"],
        action='store')
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()
    dbg.init_logger()
    _main(args)
