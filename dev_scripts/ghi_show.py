#!/usr/bin/env python

"""
Simple wrapper around ghi (GitHub Interface) to implement some typical
workflows.

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


# Get all the data relative to issue #13 for a different GitHub repo:
> ghi_show.py 13 --repo Amp
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


def _parse_issue_title(txt):
    m = re.match("#(\d+):\s*(.*)\s*", txt)
    issue_num = m.group(1)
    issue_num = int(issue_num)
    # Extract the bug subject.
    title = m.group(2)
    _LOG.debug("txt=%s ->\n  issue_num=%s\n  title=%s", txt, issue_num, title)
    # Remove some annoying chars.
    for char in ": + ( ) /".split():
        title = title.replace(char, "")
    # Replace multiple spaces with one.
    title = re.sub("\s+", " ", title)
    #
    title = title.replace(" ", "_")
    return issue_num, title


def _print_github_info(issue_num, repo_github_name):
    print(pri.color_highlight("# Github:", "green"))
    ghi_opts = ""
    if repo_github_name is not None:
        ghi_opts = "-- %s" % repo_github_name
    cmd = "ghi show %d %s | head -1" % (issue_num, ghi_opts)
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)
    # Print url.
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    url_name = "https://github.com/%s/issues/%d" % (git_repo_name, issue_num)
    print(url_name)
    # github doesn't let ping this url.
    # url.check_url(url_name)
    #
    print(pri.color_highlight("\n# Tag:", "green"))
    # TODO(gp): Add unit tests for these inputs.
    # txt = '#268: PRICE: Download metadata from CME'
    # txt = "#406: INFRA: Add decorator for caching function in disk / mem"
    issue_num, title = _parse_issue_title(txt)
    #
    dbg.dassert_eq(issue_num, issue_num)
    prefix = git.get_repo_prefix(git_repo_name)
    # ParTask268
    title = prefix + "Task" + str(issue_num) + "_" + title
    print(title)


def _print_files_in_git_repo(issue_num, repo_github_name):
    _ = repo_github_name
    print(pri.color_highlight("\n# Files in the repo:", "green"))
    # regex="Task%d*\.*py*" % issue_num
    regex = r"*%d*\.*py*" % issue_num
    _LOG.debug("regex=%s", regex)
    cmd = "find . -name %s | grep -v ipynb_checkpoint" % regex
    _, txt = si.system_to_string(cmd, abort_on_error=False)
    print(txt)


def _print_gdrive_files(issue_num, repo_github_name):
    _ = repo_github_name
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
    parser.add_argument(
        "-s",
        "--repo_symbolic_name",
        action="store",
        choices=["Part", "Amp", "Lem"],
        default=None,
        help="Refer to one of the repos through a symbolic name",
    )
    parser.add_argument(
        "-r",
        "--repo_github_name",
        action="store",
        # TODO(gp): This is a workaround for PartTask551.
        default="ParticleDev/commodity_research",
        help="Refer to one of the repos using full git name",
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
    dbg.init_logger(verbosity=args.log_level)
    # Print url.
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    print("current_repo='%s'\n" % git_repo_name)
    # Select actions.
    actions = [_print_github_info, _print_files_in_git_repo, _print_gdrive_files]
    if args.only_github:
        actions = [_print_github_info]
    #
    repo_github_name = None
    if args.repo_symbolic_name:
        dbg.dassert_is_not(args.repo_github_name)
        repo_github_name = git.get_repo_github_name(args.repo_symbolic_name)
    elif args.repo_github_name:
        repo_github_name = args.repo_github_name
    # Scan the issues.
    for issue_num in args.positional:
        issue_num = int(issue_num)
        dbg.dassert_lte(1, issue_num)
        #
        for action in actions:
            action(issue_num, repo_github_name)


if __name__ == "__main__":
    _main(_parse())
