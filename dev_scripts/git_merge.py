#!/usr/bin/env python

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.git as git
import helpers.helper_io as io
import helpers.system_interaction as hsi

_log = logging


def _git_merge(file_name, tmp_dir_name, vs_base):
    """
    Merge a file in conflict.

    :param file_name:
    :param tmp_dir_name:
    :param vs_base: compare to base instead of theirs.

    """
    _log.info("\nResolving %s ... ", file_name)
    # Save relevant files with different versions of the same file.
    file_names = {}
    # - BASE - the common ancestor(s) of LOCAL and REMOTE.
    # - LOCAL - the head for the file(s) from the current branch on the machine
    #   that you are using.
    # - REMOTE - the head for files(s) from a remote location that you are
    #   trying to merge into your LOCAL branch.
    for id_, suffix in [("1", "base"), ("2", "theirs"), ("3", "mine")]:
        # Save file.
        dst_file_name = "%s/%s.%s" % (tmp_dir_name, os.path.basename(file_name),
                                      suffix)
        cmd = "git show :%s:%s >%s" % (id_, file_name, dst_file_name)
        hsi.system(cmd)
        if file_name.endswith(".ipynb"):
            # Apply nbstripout.
            cmd = "nbstripout -f %s" % dst_file_name
            hsi.system(cmd)
        file_names[suffix] = dst_file_name
    # Diff.
    if vs_base:
        lhs = file_names["base"]
    else:
        lhs = file_names["theirs"]
    rhs = file_names["mine"]
    cmd = "vimdiff %s %s" % (lhs, rhs)
    _log.debug(">> %s", cmd)
    # Do not redirect to file when using vimdiff.
    os.system(cmd)
    #
    ans = eval(input("Resolved? [y/n] "))
    if ans.rstrip(" ").lstrip(" ") in ("y", "yes"):
        # Make a backup.
        root_dir = git.get_client_root()
        client_file_name = "%s/%s" % (root_dir, file_name)
        cmd = "cp %s %s.bak" % (client_file_name, client_file_name)
        hsi.system(cmd)
        # Overwrite.
        cmd = "cp -r %s %s" % (file_names["mine"], client_file_name)
        hsi.system(cmd)
        # Add to resolve and then unstage.
        cmd = "git add %s" % client_file_name
        hsi.system(cmd)
        cmd = "git reset HEAD -- %s" % client_file_name
        hsi.system(cmd)
        _log.info("RESOLVED")
    else:
        _log.warning("NOT RESOLVED")


def _main(args):
    dbg.init_logger2(args.log_level)
    # Find list of files.
    if not args.file:
        # Find files in conflict.
        cmd = "git diff --name-only --diff-filter=U"
        _, txt = hsi.system_to_string(cmd)
        file_names = txt.split("\n")
        dbg.dassert_lte(1, len(file_names))
    else:
        file_names = args.file
    _log.info("# %s files to resolve:\n%s\n", len(file_names),
              "\n".join(file_names))
    # Resolve files.
    tmp_dir_name = "./tmp.git_merge"
    io.create_dir(tmp_dir_name, incremental=False)
    for file_name in file_names:
        _git_merge(file_name, tmp_dir_name, args.vs_base)


def _parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, action='append')
    parser.add_argument(
        '--vs_base',
        action='store_true',
        help="Compare to the base / ancestor version instead of the theirs /"
        " remote version")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()
    _main(args)


if __name__ == "__main__":
    _parse()