#!/usr/bin/env python
"""
Install and remove git pre-commit hooks.

# Install hooks.
> install_hooks.py --action install

# Remove hooks.
> install_hooks.py --action remove

# Check hook status.
> install_hooks.py --action status
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

_HOOKS = ["pre-commit", "commit-msg"]


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["install", "remove", "status"],
        action="store",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    git_root = git.get_client_root()
    _LOG.info("git_root=%s", git_root)
    target_dir = git_root + "/.git/hooks"
    _LOG.info("target_dir=%s", target_dir)
    dbg.dassert_exists(target_dir)
    #
    if args.action == "install":
        _LOG.info("Installing hooks to %s", target_dir)
    elif args.action == "remove":
        _LOG.info("Removing hooks from %s", target_dir)
    elif args.action == "status":
        _LOG.info("Checking status of hooks in %s", target_dir)
        cmd = "ls -l %s" % target_dir
        si.system(cmd, suppress_output=False, log_level=logging.DEBUG)
        _LOG.info("Done")
        sys.exit(0)
    else:
        dbg.dfatal("Invalid action='%s'" % args.action)
    #
    for hook in _HOOKS:
        target_file = os.path.join(target_dir, hook)
        _LOG.debug("target_file=%s", target_file)
        if args.action == "install":
            hook_file = os.path.join(git_root, "dev_scripts/git_hooks", hook)
            hook_file += ".py"
            hook_file = os.path.abspath(hook_file)
            dbg.dassert_exists(hook_file)
            cmd = "ln -sf %s %s" % (hook_file, target_file)
            si.system(cmd, log_level=logging.DEBUG)
            cmd = "chmod +x %s" % hook_file
            si.system(cmd, log_level=logging.DEBUG)
            cmd = "chmod +x %s" % target_file
            si.system(cmd, log_level=logging.DEBUG)
        elif args.action == "remove":
            if os.path.exists(target_file):
                cmd = "unlink %s" % target_file
                si.system(cmd, log_level=logging.DEBUG)
            else:
                _LOG.warning("Nothing to do since %s doesn't exist", target_file)
        else:
            dbg.dfatal("Invalid action='%s'" % args.action)
    _LOG.info("Done")


if __name__ == "__main__":
    _main()
