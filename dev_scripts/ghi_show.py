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
import re

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

_COLOR = "green"


def _print_github_info(issue_num):
    print(pri.color_highlight("# Github:", "green"))
    cmd = "ghi show %d | head -1" % issue_num
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)
    # Print url.
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    url_name = "https://github.com/%s/issues/%d" % (git_repo_name, issue_num)
    print(url_name)
    # github doesn't let ping this url.
    # url.check_url(url_name)
    #
    # txt = '#268: PRICE: Download metadata from CME'
    m = re.match("#(\d+):\s*(.*)\s*", txt)
    dbg.dassert(m, "Invalid txt='%s'", txt)
    #
    dbg.dassert_eq(int(m.group(1)), issue_num)
    title = m.group(2)
    for char in ": + ( )".split():
        title = title.replace(char, "")
    title = title.replace(" ", "_")
    #
    prefix = git.get_repo_prefix(git_repo_name)
    # ParTask268
    title = prefix + "Task" + str(issue_num) + "_" + title
    print("\n" + title)


def _print_files_in_git_repo(issue_num):
    print(pri.color_highlight("\n# Files in the repo:", "green"))
    # regex="Task%d*\.*py*" % issue_num
    regex = r"*%d*\.*py*" % issue_num
    _LOG.debug("regex=%s", regex)
    cmd = "find . -name %s | grep -v ipynb_checkpoint" % regex
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)


def _print_gdrive_files(issue_num):
    env_name = "P1_GDRIVE_PATH"
    if env_name in os.environ:
        dir_name = os.environ[env_name]
        print(
            pri.color_highlight(
                "\n# Files in the gdrive '%s':" % dir_name, "green"
            )
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


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--only_github", action="store_true", help="Print only git hub info"
    )
    parser.add_argument("positional", nargs="+", help="Github issue number")
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
    actions = [_print_github_info, _print_files_in_git_repo, _print_gdrive_files]
    if args.only_github:
        actions = [_print_github_info]

    for issue_num in args.positional:
        issue_num = int(issue_num)
        dbg.dassert_lte(1, issue_num)
        #
        for action in actions:
            action(issue_num)


if __name__ == "__main__":
    _main(_parse())
