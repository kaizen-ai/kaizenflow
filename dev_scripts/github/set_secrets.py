#!/usr/bin/env python
"""
Script to set batch of GitHub secrets from `.json` file in one go.

Simple usage:

> ./dev_scripts/github/set_secrets.py \
     --file 'dev_scripts/github/secrets.json' \
     --repo 'cryptomtc/cmamp_test'
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    secrets = hio.from_json(args.file)
    for secret_key, secret_value in secrets.items():
        cmd = [
            f"gh secret set {secret_key}",
            f'--body "{secret_value}"',
            f"--repo {args.repo}",
        ]
        hsystem.system(" ".join(cmd))
        _LOG.info("%s is set!", secret_key)
    _LOG.info("All secrets are set!")


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    parser.add_argument(
        "--file",
        action="store",
        type=str,
        help="Location of `.json` file with desired secrets.",
    )
    parser.add_argument(
        "--repo",
        action="store",
        type=str,
        help="On which repository command will be applied.",
    )
    return parser


if __name__ == "__main__":
    _main(_parse())
