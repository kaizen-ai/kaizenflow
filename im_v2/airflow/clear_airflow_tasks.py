#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT.

Use as:

# Download OHLCV data for binance 'v03', saving dev_stage:
> im_v2/airflow/clear_airflow_tasks.py \
    --mwaa_environment 'Crypto_Airflow' \
    --dag_duration_threshold '180' \
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3

import boto3
from botocore.client import BaseClient
import json
import requests 
import base64
import datetime

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = hparser.add_verbosity_arg(parser)
    parser.add_argument(
        "--mwaa_environment",
        action="store",
        required=True,
        type=str,
        help="Name of MWAA Environment to search in",
    )
    parser.add_argument(
        "--dag_duration_threshold",
        action="store",
        required=True,
        type=int,
        help="Max allowed duration (in seconds) for DAGs, Tasks in DAGs running \
            longer than this threshold will get cleared to unblock worker assigned to it",
    )

    return parser  # type: ignore[no-any-return]

def _get_mwaa_client(*, aws_profile: str = "ck") -> BaseClient:
    """
    Return client to work with AWS MWAA in the specified region.
    """
    hdbg.dassert_isinstance(aws_profile, str)
    session = boto3.session.Session(profile_name=aws_profile)
    client = session.client(service_name="mwaa")
    return client

def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)

    client = _get_mwaa_client()

    mwaa_cli_token = client.create_cli_token(
        Name=args.mwaa_environment
    )
    mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])
    mwaa_cli_command = 'dags list -o json'
    mwaa_response = requests.post(
            mwaa_webserver_hostname,
            headers={
                'Authorization': mwaa_auth_token,
                },
            data=mwaa_cli_command
    )
            
    mwaa_std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
    mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
    dags_list = json.loads(mwaa_std_out_message)


    dags_list = filter(lambda x: x["paused"] == "False", dags_list)
    dags_list = list(map(lambda x: x["dag_id"], dags_list))
    for dag in dags_list:
        start_datetime = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=args.dag_duration_threshold)
        start_datetime = start_datetime.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        _LOG.info(f"Start: {start_datetime}")
        # bug in the implementation?
        mwaa_cli_command = f'dags list-runs --dag-id {dag} -o json --end-date {start_datetime} --state running'
        # mwaa_cli_command = f'dags list-runs --dag-id {dag} -o json --end-date {start_datetime} --state running'
        #mwaa_cli_command = f'dags list-runs --dag-id {dag} -o json --state running'
        mwaa_response = requests.post(
            mwaa_webserver_hostname,
            headers={
                'Authorization': mwaa_auth_token,
                },
            data=mwaa_cli_command
            )
        _LOG.info(mwaa_response.status_code)
        mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
        dag_runs = json.loads(mwaa_std_out_message)
        _LOG.info(dag_runs)
        if dag == "test_mock_long_dag":
            _LOG.info(f"Clearing {dag}")
            #mwaa_cli_command = f'tasks clear -r --start-date {start_datetime} {dag}'
            mwaa_cli_command = f'tasks clear -y --only-running --end-date {start_datetime} {dag}'
            mwaa_response = requests.post(
                mwaa_webserver_hostname,
                headers={
                    'Authorization': mwaa_auth_token,
                    },
                data=mwaa_cli_command
            )
            _LOG.info("Clearing: result:", mwaa_response.status_code)
            _LOG.info(mwaa_response.text)
            mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
            _LOG.info(mwaa_std_out_message)

if __name__ == "__main__":
    _main(_parse())


