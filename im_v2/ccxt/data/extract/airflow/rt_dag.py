"""
Import as:

import im_v2.ccxt.data.extract.airflow.rt_dag as imvcdearda
"""

import datetime

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator

# Set ECS configuration.
ecs_cluster = "Crypto1"
ecs_task_definition = "cmamp"
ecs_subnets = ["subnet-0d7a4957ff09e7cc5", "subnet-015eee0c93f916f23"]
ecs_security_group = ["sg-0c605e9a7bb0df2aa"]
ecs_awslogs_group = "/ecs/cmamp"
ecs_awslogs_stream_prefix = "ecs/cmamp"


# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
    "email_on_failure": False,
    "owner": "airflow",
}

# Create a command.
bash_command = [
    "im_v2/ccxt/data/extract/download_realtime_data.py",
    "--to_datetime {{ next_execution_date }}",
    "--from_datetime {{ execution_date - macros.timedelta(5) }}"
    # TODO(Danya): Set a shared directory for the DAG (CMTask675).
    "--dst_dir 'ccxt/ohlcv/'",
    "--data_type 'ohlcv'",
    "--universe 'v03'",
    "--db_stage 'dev'",
    "--v DEBUG",
]


# Create a DAG.
dag = airflow.DAG(
    dag_id="realtime_ccxt",
    description="Realtime download of CCXT OHLCV data",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    start_date=airflow.utils.date.days_ago(0),
)


# Run the script with ECS operator.
downloading_task = ECSOperator(
    task_id="realtime_ccxt",
    dag=dag,
    aws_conn_id=None,
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "cmamp",
                "command": bash_command,
                "environment": [
                    {
                        "name": "DATA_INTERVAL_START",
                        "value": "{{ execution_date }}",
                    },
                    {
                        "name": "DATA_INTERVAL_END",
                        "value": "{{ execution_date - macros.timedelta(5) }}",
                    },
                ],
            }
        ]
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ecs_security_group,
            "subnets": ecs_subnets,
        },
    },
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
)
# Execute the DAG.
downloading_task  # pylint: disable=pointless-statement
