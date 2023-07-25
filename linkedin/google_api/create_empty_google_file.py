#!/usr/bin/env python

"""
Convert Docx file to Markdown.

Example:
Run this command in the same directory as the Markdown file:
> create_empty_google_file.py --gfile_type sheet --gfile_name test_new
--folder_name LinkedIn
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import linkedin.google_api.google_file_api as gapi

_LOG = logging.getLogger(__name__)

# #############################################################################

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--gfile_type",
        action="store",
        required=True,
        type=str,
        help="The Docx file that needs to be converted to Markdown",
    )
    parser.add_argument(
        "--gfile_name",
        action="store",
        required=True,
        type=str,
        help="The output Markdown file",
    )
    parser.add_argument(
        "--folder_id",
        action="store",
        required=False,
        type=str,
        help="The output Markdown file",
    )
    parser.add_argument(
        "--folder_name",
        action="store",
        required=False,
        type=str,
        help="The output Markdown file",
    )
    parser.add_argument(
        "--user",
        action="store",
        required=False,
        type=str,
        help="The output Markdown file",
    )
    hparser.add_verbosity_arg(parser)
    return parser

def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _LOG.info("Start create empty google file...")
    gapi.create_empty_google_file(
        args.gfile_type, 
        args.gfile_name, 
        args.folder_id, 
        args.folder_name, 
        args.user
    )


if __name__ == "__main__":
    _main(_parse())
