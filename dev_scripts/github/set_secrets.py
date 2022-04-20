#!/usr/bin/env python
"""
Script to set batch of GitHub secrets from `.json` file in one go.

Simple usage:

> ./dev_scripts/github/set_secrets.py \
     --file 'dev_scripts/github/secrets.json' \
     --repo 'cryptomtc/cmamp_test'

The json file looks like:
```
{
    'AM_AWS_ACCESS_KEY_ID': '?',
    'AM_AWS_DEFAULT_REGION': 'us-east-1',
    'GH_ACTION_AWS_SECRET_ACCESS_KEY': ''
}
```
"""

import argparse
import logging
import pprint
import sys

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


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
        "--dry_run",
        action="store_true",
    )
    parser.add_argument(
        "--remove",
        action="store_true",
    )
    parser.add_argument(
        "--repo",
        action="store",
        type=str,
        help="On which repository command will be applied.",
    )
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    secrets = hio.from_json(args.file)
    #
    if args.dry_run:
        print(pprint.pformat(secrets))
        sys.exit(0)
    hdbg.dassert(args.repo)
    # TODO(gp): set them in sorted.
    for secret_key, secret_value in secrets.items():
        hdbg.dassert_ne(secret_value, "")
        if args.remove:
            try:
                cmd = [
                    f"gh secret remove {secret_key}",
                    f"--repo {args.repo}",
                ]
                _LOG.debug(cmd)
                hsystem.system(" ".join(cmd))
            except RuntimeError:
                # TODO(gp): Issue a warning.
                pass
        else:
            cmd = [
                f"gh secret set {secret_key}",
                f'--body "{secret_value}"',
                f"--repo {args.repo}",
            ]
            _LOG.debug(cmd)
            hsystem.system(" ".join(cmd))
            _LOG.info("%s is set", secret_key)
    _LOG.info("All secrets are set!")


if __name__ == "__main__":
    _main(_parse())
