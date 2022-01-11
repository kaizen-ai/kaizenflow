"""
Import as:

import im_v2.ccxt.data.extract.dags.rt_dag as imvcdedrda
"""

import datetime

import airflow
import boto3

# Set up boto client.
ssm = boto3.client("ssm")
# Set ECS configuration.
ecs_config = {
    "cluster": str(
        ssm.get_parameter(Name="/mwaa/ecs/cluster", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
    "task_definition": str(
        ssm.get_parameter(Name="/mwaa/ecs/cluster", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
    "subnets": str(
        ssm.get_parameter(Name="/mwaa/vpc/private_subnets", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
    "security_group": str(
        ssm.get_parameter(Name="/mwaa/vpc/security_group", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
    "awslogs_group": str(
        ssm.get_parameter(Name="/mwaa/cw/log_group", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
    "awslogs_stream_prefix": str(
        ssm.get_parameter(Name="/mwaa/cw/log_stream", WithDecryption=True)[
            "Parameter"
        ]["Value"]
    ),
}


# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "email_on_failure": False,
    "owner": "airflow",
}

with airflow.DAG(
    dag_id="realtime_ccxt",
    description="Realtime download of CCXT OHLCV data",
    max_active_runs=1,
    default_args=default_args,
    # TODO(Danya): Improve the runtime of the script to fit into 1 minute.
    schedule_interval="*/3 * * * *",
    catchup=False,
    # TODO(Danya): Change to airflow.utils.dates method.
    start_date=datetime.datetime.now(),
) as dag:
    # Pass default parameters for the script.
    # Build a bash command to execute.
    bash_command = " ".join(
        [
            "im_v2/ccxt/data/extract/download_realtime.py",
            "--to_datetime {{ next_execution_date }}",
            "--from_datetime {{ execution_date - macros.timedelta(5) }}"
            # TODO(Danya): Set a shared directory for the DAG (#675).
            "--dst_dir 'ccxt/ohlcv/'",
            "--data_type 'ohlcv'",
            "--api_keys 'API_keys.json'",
            "--universe 'v03'",
            "--v DEBUG",
        ]
    )
    # Run the script.
    downloading_task = DockerOperator(
        task_id="run_ccxt_realtime",
        image="665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev",
        command=bash_command,
        default_args=default_args,
        environment={
            "DATA_INTERVAL_START": "{{ execution_date }}",
            "DATA_INTERVAL_END": "{{ next_execution_date - macros.timedelta(5) }}",
        },
    )
