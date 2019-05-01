#!/usr/bin/env python

# Check dev_scripts/linter.py to see an example of a script using this template.
"""
Add a description of what the script does and examples of command lines.
"""

import argparse
import logging

import helpers.dbg as dbg
#import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    # Insert your code here.
    # - Use log.info(), log.debug() instead of printing.
    # - Use dbg.dassert_*() for assertion.
    # - Use si.system() and si.system_to_string()


if __name__ == '__main__':
    _main()