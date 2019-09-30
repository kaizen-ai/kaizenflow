#!/usr/bin/env python

"""
# Publish all notes
> docs/scripts/publish_all_notes.py
"""


import argparse
import glob
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# ##############################################################################


def _publish_file(args, file_name, action):
    exec_path = git.find_file_in_git_tree("pandoc.py")
    dbg.dassert_exists(exec_path)
    #
    tmp_dir = args.tmp_dir
    io_.create_dir(tmp_dir, incremental=False)
    out_file = os.path.join(tmp_dir, "output.%s" % action)
    cmd = []
    cmd.append(exec_path)
    cmd.append("-a %s" % action)
    cmd.append("--input %s" % file_name)
    cmd.append("--output %s" % out_file)
    cmd.append("--tmp_dir %s" % tmp_dir)
    cmd.append("--no_open")
    cmd.append("--gdrive_dir %s" % args.dst_dir)
    cmd = " ".join(cmd)
    si.system(cmd)


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--only_html",
        action="store_true",
    )
    tmp_dir = "./tmp.publish_all_notes"
    parser.add_argument(
        "--tmp_dir",
        action="store",
        type=str,
        default=tmp_dir,
        help="Directory where to save artifacts",
    )
    dst_dir = "/Users/saggese/GoogleDriveParticle/Tech/Docs"
    parser.add_argument(
        "--dst_dir",
        action="store_true",
        default=dst_dir,
        help="Directory where to save the output",
    )
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
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    git_dir = git.get_client_root(super_module=False)
    dir_name = os.path.join(git_dir, "docs/notes/*.txt")
    file_names = glob.glob(dir_name)
    _LOG.info("Found %d files\n%s", len(file_names), prnt.space("\n".join(
        file_names)))
    #
    _LOG.info("Saving to dir '%s'", args.dst_dir)
    actions = ["html", "pdf"]
    if args.only_html:
        actions = ["html"]
    _LOG.info("actions=%s", actions)
    #
    for action in actions:
        for f in file_names:
            _LOG.info("Publishing: %s", f)
            _LOG.debug(prnt.frame("file_name=%s" % f))
            _publish_file(args, f, action)
    _LOG.info("\n" + prnt.frame("SUCCESS"))


if __name__ == "__main__":
    _main(_parse())
