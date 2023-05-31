#!/usr/bin/env python

"""
# Assigns the permissions to Sorrentum Google Drive for the contributors.

> gdrive_share.py --creds_file ~/drive.json --file_id 1LXwKpmaFW --permission_file ~/contributors.csv

Import as:

import dev_scripts.gdrive_share as dscrgdsh
"""

import argparse
import logging
import csv

import helpers.hdbg as hdbg
import helpers.hparser as hparser

import googleapiclient.discovery as gapicl
import google.oauth2.credentials as goacr

_LOG = logging.getLogger(__name__)

# #############################################################################

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--creds_file", help="Path to user credentials for the drive.")
    parser.add_argument("--file_id", help="File ID for the drive.")
    parser.add_argument("--permission_file", help="Path to a CSV of users and their permissions.")
    hparser.add_verbosity_arg(parser)
    return parser
    
def share_drive(cred_file_path, file_id, permission_file_path) -> None:
    creds = goacr.Credentials.from_authorized_user_file(cred_file_path)
    service = gapicl.build('drive', 'v3', credentials=creds)
    permission_file = open(permission_file_path, "r") 
    file_reader = csv.DictReader(permission_file)
    for row in file_reader:
        email = row[0]
        role = row[1]
        permission = {"type": "user", "role": role, "emailAddress": email}
        service.permissions().create(fileId=file_id, body=permission).execute()     
    
def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    cred_file_path = args.creds_file
    file_id = args.file_id
    permission_file_path = args.permission_file
    share_drive(cred_file_path, file_id, permission_file_path)    


if __name__ == "__main__":
    _main(_parse())