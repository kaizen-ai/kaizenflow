#!/usr/bin/env python

"""
Generate and print a bash script that is used to configure the environment.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
"""

import logging

# ##############################################################################


# This import is relative to the top of the repo.
import _bootstrap as boot
# This script is `//amp/dev_scripts/_setenv.py`, so we need ".." to go from the
# position of this executable to `//amp/helpers`.
boot.bootstrap("..")

# pylint: disable=C0413
import helpers.dbg as dbg  # isort:skip # noqa: E402

# This import is relative to the top of the repo.
import _setenv_lib as selib


_LOG = logging.getLogger(__name__)


def _main(parser):
    args = parser.parse_args()
    # TODO(gp): We might want to force to print also the name of script to
    #  help debugging in_get_logging_format(force_print_format,
    #  force_verbose_format):
    dbg.init_logger(verb=args.log_level)
    txt = []
    #
    exec_path, submodule_path, user_name = selib.report_info(txt)
    #
    # TODO(gp): After updating to anaconda3, conda doesn't work from python anymore.
    # _check_conda()
    #
    user_credentials = selib.config_git(user_name, txt)
    #
    selib.config_python(submodule_path, txt)
    #
    selib.config_conda(args, txt, user_credentials)
    #
    selib.config_bash(exec_path, txt)
    #
    selib.test_packages(exec_path, txt)
    #
    selib.save_script(args, txt)


if __name__ == "__main__":
    _main(selib.parse())
