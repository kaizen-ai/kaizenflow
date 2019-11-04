#!/usr/bin/env python

r"""
Generate and print a bash script that is used to configure the environment for
//amp client.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
"""

import logging
import os

# This import is relative to the top of the repo.
import _bootstrap as boot  # isort:skip # noqa: E402

# This script is `//amp/dev_scripts/_setenv_amp.py`, so we need ".." to go from
# the position of this executable to `//amp/helpers`.
# pylint: disable=no-member
boot.bootstrap("..")

# pylint: disable=import-outside-toplevel,wrong-import-position
import helpers.dbg as dbg  # isort:skip # noqa: E402
import helpers.user_credentials as usc  # isort:skip # noqa: E402
import _setenv_lib as selib  # isort:skip # noqa: E402


# ##############################################################################


_LOG = logging.getLogger(__name__)


def _main(parser):
    args = parser.parse_args()
    # TODO(gp): We might want to force to print also the name of script to
    #  help debugging in_get_logging_format(force_print_format,
    #  force_verbose_format):
    dbg.init_logger(verb=args.log_level)
    txt = []
    #
    # - Report system info.
    #
    client_root_dir, user_name = selib.report_info(txt)
    # Get the path to amp dir.
    amp_path = client_root_dir
    amp_path = os.path.abspath(amp_path)
    dbg.dassert_exists(amp_path)
    #
    # - Config Git.
    #
    user_credentials = usc.get_credentials()
    selib.config_git(user_name, user_credentials, txt)
    #
    # - Config Python (e.g., PYTHONPATH).
    #
    selib.config_python([client_root_dir], txt)
    #
    # - Config conda.
    #
    conda_env = "amp_develop"
    if args.conda_env:
        _LOG.warning(
            "Overriding the default conda env '%s' with '%s'",
            conda_env,
            args.conda_env,
        )
        conda_env = args.conda_env
    selib.config_conda(conda_env, user_credentials, txt)
    #
    # - Config PATH.
    #
    dirs = [
        "%s/dev_scripts/%s" % (amp_path, d)
        for d in (".", "aws", "infra", "install", "notebooks")
    ]
    selib.config_path(dirs, txt)
    #
    # - Test packages.
    #
    selib.test_packages(amp_path, txt)
    #
    # - Save.
    #
    selib.save_script(args, txt)


if __name__ == "__main__":
    _main(selib.parse())
