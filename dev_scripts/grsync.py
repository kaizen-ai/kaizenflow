#!/usr/bin/env python

# pylint: disable=C0301
"""
- Rsync a git dir against a pycharm deploy dir
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action diff
"""

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# TODO(gp):
# - Filter local, remote removing directory (first d or through rsync)
# - Mount remote file system and then get info and sha1sum

# ##############################################################################


def _get_rsync_cmd(src_dir, dst_dir, dst_ip, preview, force, execute,
                   local_to_remote):
    cmd = 'rsync'
    # -a: archive
    # -v: verbose
    # -i: itemize info
    # -z: compress
    # -u: if different keep oldest
    # -P: --partial(restart from middle if transfer was interrupted) and --progress
    # --suffix.old: use a suffix to keep old
    cmd_opts = ""
    if preview:
        assert 0
        cmd_opts += " --itemize-changes"
    if not execute:
        cmd_opts += " --dry-run"
    if force:
        cmd_opts += " -I --delete"
    cmd_opts += " -avzu"
    cmd_opts += " --exclude *.pyc --exclude .git --exclude .idea --exclude .ipynb_checkpoints"
    cmd += cmd_opts
    #dst_dir = os.path.dirname(dst_dir)
    if not local_to_remote:
        cmd += " %s %s:%s" % (src_dir, dst_ip, dst_dir)
    else:
        cmd += " %s:%s %s" % (dst_ip, dst_dir, src_dir)
    return cmd


def _get_list_files_cmd(src_dir, dst_dir, remote_user_name, dst_ip,
                        local_to_remote):
    """
    Only list files
    """
    cmd = 'rsync'
    cmd_opts = ""
    cmd_opts += " --itemize-changes"
    cmd_opts += " -n -avzu"
    cmd_opts += " --exclude .git --exclude .idea --exclude .ipynb_checkpoints"
    cmd += cmd_opts
    if not local_to_remote:
        cmd += " " + src_dir
    else:
        cmd += " %s@%s:%s" % (remote_user_name, dst_ip, dst_dir)
    cmd += " >" + ("local.txt" if not local_to_remote else "remote.txt")
    return cmd


# ##############################################################################


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--src_dir",
        action="store",
        required=True,
        help="Local directory to sync")
    parser.add_argument(
        "--dst_dir",
        action="store",
        default=None,
        help="Remote directory to sync")
    parser.add_argument(
        "--action",
        action="store",
        required=True,
        choices="rsync rsync_both_ways diff".split(),
        help="rsync from local to remote")
    parser.add_argument("--force", action="store_true", help="Force the rsync")
    parser.add_argument(
        "--preview", action="store_true", help="Use --itemize-changes")
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument(
        "--config",
        action="store",
        required=True,
        help="IP of remote machine")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    if args.config == "P1":
        remote_user_name = "gp"
        remote_ip = "104.248.187.204"
        dst_dir = "/home/gp/src/commodity_research"
    else:
        raise ValueError("Invalid config='%s'" % args.config)
    if args.dst_dir:
        dst_dir = args.dst_dir
    dbg.dassert_is_not(dst_dir, None)
    # Check that both dirs exist.
    dbg.dassert_is_not(args.src_dir, None)
    src_dir = os.path.abspath(args.src_dir)
    dbg.dassert_exists(src_dir)
    #
    print("src_dir=%s" % args.src_dir)
    print("dst_dir=%s" % dst_dir)
    print("remote=%s@%s" % (remote_user_name, remote_ip))
    #
    cmd = 'ssh %s@%s "ls %s"' % (remote_user_name, remote_ip, dst_dir)
    rc = si.system(cmd, abort_on_error=False)
    if rc != 0:
        msg = "Can't find remote dir '%s' on '%s@%s'" % (dst_dir,
                                                      remote_user_name,
                                                         remote_ip)
        _LOG.error(msg)
        raise RuntimeError(msg)
    #
    if args.action == "rsync":
        force = args.force
        preview = args.preview
        execute = not args.dry_run
        local_to_remote = False
        cmd = _get_rsync_cmd(src_dir, dst_dir, remote_ip, preview,
                             force, execute, local_to_remote)
        si.system(cmd, suppress_output=False)
    elif args.action == "rsync_both_ways":
        raise RuntimeError("Not implemented yet")
        force = False
        preview = args.preview
        execute = not args.dry_run
        #
        local_to_remote = False
        cmd = _get_rsync_cmd(src_dir, dst_dir, remote_ip, preview,
                             force, execute, local_to_remote)
        si.system(cmd, suppress_output=False)
        #
        local_to_remote = True
        cmd = _get_rsync_cmd(src_dir, dst_dir, remote_ip, preview,
                             force, execute, local_to_remote)
        si.system(cmd, suppress_output=False)
    elif args.action == "diff":
        local_to_remote = False
        cmd = _get_list_files_cmd(src_dir, dst_dir, remote_user_name, remote_ip,
                                  local_to_remote)
        si.system(cmd, suppress_output=False)
        #
        local_to_remote = True
        cmd = _get_list_files_cmd(src_dir, dst_dir, remote_user_name, remote_ip,
                                  local_to_remote)
        si.system(cmd, suppress_output=False)
    else:
        dbg.dfatal("Invalid action='%s'" % args.action)


if __name__ == '__main__':
    _main()