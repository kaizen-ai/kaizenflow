#!/usr/bin/env python

"""
Generate and print a bash script that is used to configure the environment.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.user_credentials as usc

# Store the values before any modification.
_PATH = os.environ["PATH"].copy()
_PYTHONPATH = os.environ["PYTHONPATH"].copy()


def _config_env():
    """
    Tweak PYTHONPATH to pick up amp, even if we are configuring amp, breaking the
    circular dependency.
    """
    # This script is in dev_scripts, which is at the same level of helpers.
    exec_name = os.path.abspath(sys.argv[0])
    _HELPERS_PATH = os.path.abspath(os.path.dirname(exec_name) + "/../helpers")
    assert os.path.exists(_HELPERS_PATH), "Can't find '%s'" % _HELPERS_PATH
    os.environ["PYTHONPATH"] = _HELPERS_PATH + ":" + os.environ["PYTHONPATH"]


_config_env()


_LOG = logging.getLogger(__name__)

# # Sample at the beginning of time before we start fiddling with command line
# # args.
# _CMD_LINE = " ".join(arg for arg in sys.argv)
#
#
# # From dbg.
# def get_command_line():
#     return _CMD_LINE
#
#
# # From dbg.
# def init_logger(verb=logging.INFO,):
#     root_logger = logging.getLogger()
#     # Set verbosity for all loggers.
#     root_logger.setLevel(verb)
#     root_logger.getEffectiveLevel()
#     #
#     ch = logging.StreamHandler(sys.stdout)
#     ch.setLevel(verb)
#     # TODO(gp): Print at much 15-20 chars of a function so that things are
#     # aligned.
#     # log_format = "%(levelname)-5s: %(funcName)-15s: %(message)s"
#     log_format = "%(asctime)-5s: %(levelname)s: %(funcName)s: %(message)s"
#     date_fmt = "%Y-%m-%d %I:%M:%S %p"
#     formatter = logging.Formatter(log_format, datefmt=date_fmt)
#     ch.setFormatter(formatter)
#     root_logger.addHandler(ch)
#
#
# # From system_interaction.
# def get_user_name():
#     import getpass
#
#     res = getpass.getuser()
#     return res
#
#
# # From system_interaction.
# def get_server_name():
#     res = os.uname()
#     # posix.uname_result(
#     #   sysname='Darwin',
#     #   nodename='gpmac.lan',
#     #   release='18.2.0',
#     #   version='Darwin Kernel Version 18.2.0: Mon Nov 12 20:24:46 PST 2018;
#     #       root:xnu-4903.231.4~2/RELEASE_X86_64',
#     #   machine='x86_64')
#     # This is not compatible with python2.7
#     # return res.nodename
#     return res[1]


def _remove_redundant_paths(var):
    paths = var.split(":")
    # Set of unique paths.
    found_paths = set()
    paths_out = []
    for p in paths:
        if p == "":
            continue
        if p not in found_paths:
            # Found a new path. Add it to the output.
            found_paths += p
            paths_out.append(p)
    return paths_out


def _print_paths(var):
    paths = var.split(":")
    return "\n".join(paths)


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-e", "--conda_env", default="develop", help="Select the conda env to use"
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    # To capture some dummy command options (e.g., "bell-style none") passed by
    # source on some systems.
    parser.add_argument("positional", nargs="*", help="...")
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    txt = []

    def _frame(comment):
        txt_tmp = []
        txt_tmp.append("# " + "#" * 80)
        txt_tmp.append("# " + comment)
        txt_tmp.append("# " + "#" * 80)
        txt.extend(txt_tmp)

    def _log_var(var_name, var_val):
        txt.append("# %s='%s'" % (var_name, var_val))
        _LOG.debug("%s='%s'", var_name, var_val)

    #
    # Info.
    #
    _frame("Info")
    _log_var("cmd_line", dbg.get_command_line())
    # Full path of this executable.
    exec_name = os.path.abspath(sys.argv[0])
    dbg.dassert_dir_exists(exec_name)
    _log_var("exec_name", exec_name)
    # TODO(gp): Use git to make sure you are in the root of the repo to
    # configure the environment.
    exec_path = os.path.dirname(exec_name)
    _log_var("exec_path", exec_path)
    # Current dir.
    curr_path = os.getcwd()
    _log_var("curr_path", curr_path)
    # Get name.
    user_name = si.get_user_name()
    _log_var("user_name", user_name)
    server_name = si.get_server_name()
    _log_var("server_name", server_name)
    #
    # Config git.
    #
    _frame("Config git")
    user_credentials = usc.get_credentials(user_name, server_name)
    git_user_name = user_credentials["git_user_name"]
    _log_var("git_user_name", git_user_name)
    git_user_email = user_credentials["git_user_email"]
    if git_user_name:
        txt.append('git config --local user.name"%s"' % git_user_name)
    if git_user_email:
        txt.append('git config --local user.email "%s"' % git_user_email)
    #
    if user_name == "jenkins":
        cmd = r"git config --list | \grep %s" % user_name
    else:
        cmd = "git config --list"
    txt.append(cmd)
    #
    # Config python.
    #
    _frame("Config python")
    #
    txt.append("# Disable python code caching.")
    txt.append("export PYTHONDONTWRITEBYTECODE=x")

    #path = [[urr_path] _PATH
    #
    txt = "\n".join(txt)
    print(txt)


if __name__ == "__main__":
    _main(_parse())
