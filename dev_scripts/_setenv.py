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

_LOG = logging.getLogger(__name__)

# Sample at the beginning of time before we start fiddling with command line
# args.
_CMD_LINE = " ".join(arg for arg in sys.argv)


# From dbg.get_command_line()
def get_command_line():
    return _CMD_LINE


# From dbg.init_logger()
def init_logger(verb=logging.INFO,):
    root_logger = logging.getLogger()
    # Set verbosity for all loggers.
    root_logger.setLevel(verb)
    root_logger.getEffectiveLevel()
    #
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(verb)
    # TODO(gp): Print at much 15-20 chars of a function so that things are
    # aligned.
    # log_format = "%(levelname)-5s: %(funcName)-15s: %(message)s"
    log_format = "%(asctime)-5s: %(levelname)s: %(funcName)s: %(message)s"
    date_fmt = "%Y-%m-%d %I:%M:%S %p"
    formatter = logging.Formatter(log_format, datefmt=date_fmt)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)


# From system_interaction.get_user_name()
def get_user_name():
    import getpass

    res = getpass.getuser()
    return res


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
    init_logger(verb=args.log_level)
    #
    _LOG.debug("cmd_line=%s", get_command_line())
    # Full path of this executable.
    exec_name = os.path.abspath(sys.argv[0])
    _LOG.debug("exec_name=%s", exec_name)
    assert os.path.exists(exec_name), "exec_name=%s" % exec_name
    #
    exec_path = os.path.dirname(exec_name)
    _LOG.debug("exec_path=%s", exec_path)
    # Current dir.
    dir_name = os.getcwd()
    _LOG.debug("dir_name=%s", dir_name)
    #
    txt = []
    txt.append("export PATH2='test'")
    txt.append("export PATH3='test'")
    txt = "\n".join(txt)
    #
    print(txt)


if __name__ == "__main__":
    _main(_parse())
