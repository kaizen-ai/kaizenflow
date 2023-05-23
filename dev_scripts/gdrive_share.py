#!/usr/bin/env python

"""
# Assigns the permissions to Sorrentum Google Drive for the contributors.

> gdrive_share.py nfarber1@umd.edu

Import as:

import dev_scripts.gdrive_share as dscrgdsh
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser

import googleapiclient.discovery as gapicl
import google.oauth2.credentials as goacr

_log = logging.getLogger(__name__)

# #############################################################################

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParse(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatt
er
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    hparser.add_verbosity_arg(parser)
    return parser
    

def share_drive(file_id, email) -> none:
    creds = None
    service = gapicl('drive', 'v3', credentials=creds)
    permission = {'type': 'user', 'role': 'write', 'emailAddress': email}
    service.permissions().create(fileId=file_id, body=permission).execute()

def _main(parser: argparse.ArgumentParser) -> none:
    args = parser.parse_args()
    positional = args.positional
    file_id = "Sorrentum - Contributors"
    email = positional[1]
    share_drive(file_id, email)    


if __name__ == "__main__":
    _main(_parse())