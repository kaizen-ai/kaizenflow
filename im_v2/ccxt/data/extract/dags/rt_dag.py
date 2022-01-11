"""
Import as:

import im_v2.ccxt.data.extract.dags.rt_dag as imvcdedrda
"""

import datetime

import airflow
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ecs_operator import ECSOperator

# Set ECS configuration.
ecs_cluster = "Crypto1"
ecs_task_definition = "cmamp1"
ecs_subnets = ["subnet-0d7a4957ff09e7cc5", "subnet-015eee0c93f916f23"]
ecs_security_group = ["sg-0c605e9a7bb0df2aa"]
ecs_awslogs_group = "/ecs/airflow-ecs-operator"
ecs_awslogs_stream_prefix = "ecs"


# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "email_on_failure": False,
    "owner": "airflow",
}

# Create a command.
bash_command = [
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


# Create a DAG.
dag = airflow.DAG(
    dag_id="realtime_ccxt",
    description="Realtime download of CCXT OHLCV data",
    max_active_runs=1,
    default_args=default_args,
    # TODO(Danya): Improve the runtime of the script to fit into 1 minute.
    schedule_interval="*/3 * * * *",
    catchup=False,
    #start_date=days_ago(1),
    start_date = datetime.datetime.now() + datetime.timedelta(minutes=3)
)


# Run the script with ECS operator.
downloading_task = ECSOperator(
    task_id="run_ccxt_realtime",
    dag=dag,
    aws_conn_id=None,
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="FARGATE",
    overrides={
        "ContainerOverrides": [
            {
                "name": "run_ccxt_realtime",
                "command": bash_command,
                "environment": {
                    "DATA_INTERVAL_START": "{{ execution_date }}",
                    "DATA_INTERVAL_END": "{{ next_execution_date - macros.timedelta(5) }}",
                },
            }
        ]
    },
    command=bash_command,
    default_args=default_args,
    environment={
        "DATA_INTERVAL_START": "{{ execution_date }}",
        "DATA_INTERVAL_END": "{{ next_execution_date - macros.timedelta(5) }}",
    },
)

downloading_task