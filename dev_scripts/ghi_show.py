#!/usr/bin/env python

"""
# Get all the data relative to issue #257:
> ghi_show.py 257
# Github:
#257: ST: sources analysis
https://github.com/ParticleDev/commodity_research/issues/257

# Files in the repo:
./oil/ST/Task257_Sources_analysis.py
./oil/ST/Task257_Sources_analysis.ipynb

# Files in the gdrive '/Users/saggese/GoogleDriveParticle':
'/Users/saggese/GoogleDriveParticle/Tech/Task 257 - ST - Sources Analysis.gdoc'
  https://docs.google.com/open?id=1B70mA0m5UovKmuzAq05XESlKNvToflBR1uqtcYGXhhM
"""

import argparse
import json
import logging
import os

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, help="Github issue number")
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
    dbg.init_logger(verb=args.log_level)
    #
    dbg.dassert_eq(len(args.positional), 1)
    issue_num = int(args.positional[0])
    dbg.dassert_lte(1, issue_num)
    #
    print(pri.color_highlight("# Github:", "red"))
    cmd = "ghi show %d | head -1" % issue_num
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)
    # Print url.
    git_repo_name = git.get_repo_symbolic_name()
    url_name = "https://github.com/%s/issues/%d" % (git_repo_name, issue_num)
    print(url_name)
    # github doesn't let ping this url.
    # url.check_url(url_name)
    #
    print(pri.color_highlight("\n# Files in the repo:", "red"))
    # regex="Task%d*\.*py*" % issue_num
    regex = r"*%d*\.*py*" % issue_num
    _LOG.debug("regex=%s", regex)
    cmd = "find . -name %s | grep -v ipynb_checkpoint" % regex
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)
    #
    env_name = "P1_GDRIVE_PATH"
    if env_name in os.environ:
        dir_name = os.environ[env_name]
        print(
            pri.color_highlight("\n# Files in the gdrive '%s':" % dir_name, "red")
        )
        regex = r"*%d*" % issue_num
        _LOG.debug("regex=%s", regex)
        cmd = "find %s -name %s" % (dir_name, regex)
        _, txt = si.system_to_string(cmd, abort_on_error=False)
        # Get links.
        # {
        #   "url":
        #       "https://docs.google.com/open?id=1B70mA0m5UovKmuzAq05XESlKNvToflBR1uqtcYGXhhM",
        #   "doc_id":
        #       "1B70mA0m5UovKmuzAq05XESlKNvToflBR1uqtcYGXhhM",
        #   "email":
        #       "gp@particle.one"}
        for f in txt.split("\n"):
            if f.endswith(".gdoc") or f.endswith(".gsheet"):
                txt_tmp = io_.from_file(f, split=False)
                dict_ = json.loads(txt_tmp)
                print("'%s'" % f)
                print("  %s" % dict_["url"])
    else:
        _LOG.warning("No env var '%s' so can't look for files there", env_name)


if __name__ == "__main__":
    _main(_parse())
