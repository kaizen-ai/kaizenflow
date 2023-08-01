#!/usr/bin/env python

"""
Assigns the permissions to Sorrentum Google Drive for the contributors.

> dev_scripts/gdrive_share.py \
    --credentials creds.json \
    --file_id 10101 \
    --permission_file contributors.csv

Import as:

import dev_scripts.gdrive_share as dscrgdsh
"""

import argparse
import logging
import os

try:
    import google.oauth2 as goa
    import googleapiclient.discovery as gapicld
    import pandas as pd
except ImportError:
    pass

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _run_docker(
    cred_file_path: str, file_id: str, permission_file_path: str
) -> None:
    dockerfile_path = "Dockerfile"
    with open(dockerfile_path, "w+") as dockerfile:
        dockerfile.write(
            """
                FROM ubuntu:20.04

        RUN apt update && apt install -y python3 python3-pip
        ENV PYTHONPATH=$PYTHONPATH:./helpers
                RUN pip install google-auth google-api-python-client pandas

        WORKDIR /app

                COPY . /app

                CMD python3 dev_scripts/share_grive.py --permission_file \
        {permission_file_path} \
                --file_id {file_id} --credentials {cred_file_path}
        """
        )
        dockerfile.flush()
        cmd = f"docker build -f {dockerfile.name} -t share_grive_docker ."
        hsystem.system(cmd)
    os.getcwd()
    docker_target_dir = "/app"
    docker_cmd = (
        f"docker run --rm -it --workdir {docker_target_dir} share_grive_docker"
    )
    hsystem.system(docker_cmd)


def _share_grive(
    cred_file_path: str, file_id: str, permission_file_path: str
) -> None:
    """
    Share Google Drive permissions.
    """
    try:
        creds = goa.service_account.Credentials.from_service_account_file(
            cred_file_path
        )
        service = gapicld.build("drive", "v3", credentials=creds)
        permission_df = pd.read_csv(permission_file_path)
        for _, row in permission_df.iterrows():
            email = row["emailAddress"]
            role = row["role"]
            permission = {"type": "user", "role": role, "emailAddress": email}
            service.permissions().create(
                fileId=file_id, body=permission
            ).execute()
    except Exception:
        pass


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    hdbg.init_logger(use_exec_path=True)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--credentials",
        action="store",
        type=str,
        required=True,
        help="Path to the file with Gdrive service account credentials.",
    )
    parser.add_argument(
        "--file_id",
        action="store",
        type=str,
        required=True,
        help="File ID generated for the drive.",
    )
    parser.add_argument(
        "--permission_file_path",
        action="store",
        type=str,
        required=True,
        help="Path to the CSV file with usernames and their permissions.",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    dockerfile_path = "Dockerfile"
    args = parser.parse_args()
    cred_file_path = args.credentials
    file_id = args.file_id
    permission_file_path = args.permission_file_path
    if os.path.exists(dockerfile_path):
        _share_grive(cred_file_path, file_id, permission_file_path)
    else:
        _run_docker(cred_file_path, file_id, permission_file_path)


if __name__ == "__main__":
    _main(_parse())
