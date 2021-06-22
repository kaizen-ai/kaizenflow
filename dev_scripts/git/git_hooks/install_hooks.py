#!/usr/bin/env python
"""
Manage custom git pre-commit hooks.

# Install hooks as links to the repo:
> install_hooks.py --action install

# Remove hooks:
> install_hooks.py --action remove

# Check hook status:
> install_hooks.py --action status
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# Phases to install.
# _GIT_PHASE_HOOKS = ["pre-commit", "commit-msg"]
_GIT_PHASE_HOOKS = ["pre-commit"]


def _main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["install", "remove", "status"],
        action="store",
    )
    prsr.add_verbosity_arg(parser)
    #
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    # Get amp dir.
    amp_dir = git.get_amp_abs_path()
    _LOG.info("amp_dir=%s", amp_dir)
    dbg.dassert_dir_exists(amp_dir)
    # Get the dir with the Git hooks to install.
    src_dir = os.path.join(amp_dir, "dev_scripts/git/git_hooks")
    _LOG.info("src_dir=%s", src_dir)
    dbg.dassert_dir_exists(src_dir)
    # Find the location to install the Git hooks.
    # In a super-module:
    # > git rev-parse --git-path hooks
    # .git/hooks
    # In a sub-module:
    # > git rev-parse --git-path hooks
    # /Users/saggese/src/.../.git/modules/amp/hooks
    cmd = "git rev-parse --git-path hooks"
    rc, target_dir = si.system_to_one_line(cmd)
    _ = rc
    _LOG.info("target_dir=%s", target_dir)
    dbg.dassert_dir_exists(target_dir)
    #
    if args.action == "install":
        _LOG.info("Installing hooks into '%s'", target_dir)
    elif args.action == "remove":
        _LOG.info("Removing hooks from '%s'", target_dir)
    elif args.action == "status":
        _LOG.info("Checking status of hooks in the repo '%s'", src_dir)
        cmd = "ls -l %s" % src_dir
        si.system(cmd, suppress_output=False, log_level=logging.DEBUG)
        #
        _LOG.info("Checking status of hooks in '%s'", target_dir)
        cmd = "ls -l %s" % target_dir
        si.system(cmd, suppress_output=False, log_level=logging.DEBUG)
        sys.exit(0)
    else:
        dbg.dfatal("Invalid action='%s'" % args.action)
    # Scan the hooks.
    for hook in _GIT_PHASE_HOOKS:
        # Target location for the hooks.
        target_file = os.path.join(target_dir, hook)
        _LOG.debug("target_file=%s", target_file)
        if args.action == "install":
            # The name of the script hook is the same as the hook phase.
            hook_file = os.path.join(src_dir, hook) + ".py"
            hook_file = os.path.abspath(hook_file)
            dbg.dassert_file_exists(hook_file)
            _LOG.info("Creating %s -> %s", hook_file, target_file)
            # Create link.
            cmd = "ln -sf %s %s" % (hook_file, target_file)
            si.system(cmd, log_level=logging.DEBUG)
            # Make the scripts executable.
            cmd = "chmod +x %s" % hook_file
            si.system(cmd, log_level=logging.DEBUG)
            cmd = "chmod +x %s" % target_file
            si.system(cmd, log_level=logging.DEBUG)
        elif args.action == "remove":
            _LOG.info("Remove hook '%s'", target_file)
            if os.path.exists(target_file):
                cmd = "unlink %s" % target_file
                si.system(cmd, log_level=logging.DEBUG)
            else:
                _LOG.warning(
                    "Nothing to do since '%s' doesn't exist", target_file
                )
        else:
            dbg.dfatal("Invalid action='%s'" % args.action)


if __name__ == "__main__":
    _main()
