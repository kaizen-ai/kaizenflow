#!/usr/bin/env python

"""
Generate and print a bash script that is used to configure the environment for
//amp client.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
"""

import logging
import os

# ##############################################################################


# This import is relative to the top of the repo.
import _bootstrap as boot  # isort:skip # noqa: E402

# This script is `//amp/dev_scripts/_setenv_amp.py`, so we need ".." to go from
# the position of this executable to `//amp/helpers`.
# pylint: disable=no-member
boot.bootstrap("..")

# pylint: disable=import-outside-toplevel,wrong-import-position
import helpers.dbg as dbg  # isort:skip # noqa: E402
import helpers.user_credentials as usc  # isort:skip # noqa: E402

# This import is relative to the top of the repo.
# pylint: disable=wrong-import-position
import _setenv_lib as selib  # isort:skip # noqa: E402


_LOG = logging.getLogger(__name__)


def _main(parser):
    args = parser.parse_args()
    # TODO(gp): We might want to force to print also the name of script to
    #  help debugging in_get_logging_format(force_print_format,
    #  force_verbose_format):
    dbg.init_logger(verb=args.log_level)
    txt = []
    #
    submodule_path, user_name = selib.report_info(txt)
    # Get the path to amp dir.
    amp_path = submodule_path
    amp_path = os.path.abspath(amp_path)
    dbg.dassert_exists(amp_path)
    #
    # TODO(gp): After updating to anaconda3, conda doesn't work from python anymore.
    # check_conda()
    #
    user_credentials = usc.get_credentials()
    selib.config_git(user_name, user_credentials, txt)
    #
    selib.config_python(submodule_path, txt)
    #
    selib.config_conda(args, user_credentials, txt)
    #
    dirs = [
        "dev_scripts/%s" % d
        for d in (".", "aws", "infra", "install", "notebooks")
    ]
    selib.config_bash(amp_path, dirs, txt)
    #
    selib.test_packages(amp_path, txt)
    #
    selib.save_script(args, txt)


if __name__ == "__main__":
    _main(selib.parse())
