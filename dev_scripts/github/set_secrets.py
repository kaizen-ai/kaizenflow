#!/usr/bin/env python
"""
Script to set batch of GitHub secrets in one go.

Simple usage:

> ./dev_scripts/github/set_secrets.py
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


SECRETS = {
    "GH_ACTION_ACCESS_TOKEN": "***",
    "AM_AWS_ACCESS_KEY_ID": "***",
    "AM_AWS_SECRET_ACCESS_KEY": "***",
    "AM_AWS_DEFAULT_REGION": "us-east-1",
    "AM_ECR_BASE_PATH": "***",
    "AM_AWS_S3_BUCKET": "alphamatic-data",
    "AM_TELEGRAM_TOKEN": "***",
    "CK_AWS_ACCESS_KEY_ID": "***",
    "CK_AWS_SECRET_ACCESS_KEY": "***",
    "CK_AWS_DEFAULT_REGION": "eu-north-1",
    "CK_AWS_S3_BUCKET": "cryptokaizen-data",
    "CK_TELEGRAM_TOKEN": "***",
}


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    for secret_key, secret_value in SECRETS.items():
        command = f'gh secret set {secret_key} --body "{secret_value}"'
        hsystem.system(command)
        _LOG.info("%s is set!", secret_key)
    _LOG.info("All secrets are set!")


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
