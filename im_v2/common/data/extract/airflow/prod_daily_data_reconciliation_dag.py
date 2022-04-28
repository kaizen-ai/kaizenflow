"""
This DAG is used to compare data downloaded via rt DAG with data downloaded
once per day in bulk.

Import as:

import im_v2.common.data.extract.airflow.prod_daily_data_reconciliation_dag as imvcdeapddrd
"""
import datetime

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to prod
# in one line (in best case scenario).
_STAGE = "prod"
_EXCHANGE = "binance"
_VENDOR = "ccxt"
_UNIVERSE = "v3"

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
s3_daily_staged_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_daily_staged_data_folder')}/{_VENDOR}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": True,
    "email_on_retry": True,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=f"{_STAGE}_daily_data_reconciliation",
    description="Download past day data and confirm correctness and completeness of RT data.",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="15 0 * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 2, 10, 0, 15, 0),
)

download_command = [
    f"/app/im_v2/{_VENDOR}/data/extract/download_historical_data.py",
    "--end_timestamp '{{ execution_date - macros.timedelta(minutes=15) }}'",
    "--start_timestamp '{{ execution_date - macros.timedelta(days=1) - macros.timedelta(minutes=15) }}'",
    f"--exchange_id '{_EXCHANGE}'",
    f"--universe '{_UNIVERSE}'",
    "--aws_profile 'ck'",
    f"--s3_path '{s3_daily_staged_data_path}'",
]


downloading_task = ECSOperator(
    task_id=f"daily_data_download",
    dag=dag,
    aws_conn_id=None,
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="EC2",
    overrides={
        "containerOverrides": [
            {
                "name": f"{ecs_task_definition}",
                "command": download_command,
            }
        ]
    },
    placement_strategy=[
        {"type": "spread", "field": "instanceId"},
    ],
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
    execution_timeout=datetime.timedelta(minutes=15),
)

compare_command = [
    f"/app/im_v2/{_VENDOR}/data/extract/compare_realtime_and_historical.py",
    "--end_timestamp '{{ execution_date - macros.timedelta(minutes=15) }}'",
    "--start_timestamp '{{ execution_date - macros.timedelta(days=1) - macros.timedelta(minutes=15) }}'",
    "--db_stage 'dev'",
    f"--exchange_id '{_EXCHANGE}'",
    f"--db_table '{_VENDOR}_ohlcv'",
    "--aws_profile 'ck'",
    f"--s3_path '{s3_daily_staged_data_path}'",
]

comparing_task = ECSOperator(
    task_id=f"compare_realtime_historical",
    dag=dag,
    aws_conn_id=None,
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="EC2",
    overrides={
        "containerOverrides": [
            {
                "name": f"{ecs_task_definition}",
                "command": compare_command,
            }
        ]
    },
    # This part ensures we do not get a random failure because of insufficient
    # HW resources. For unknown reasons, the ECS scheduling when using
    # your own EC2s is done  in a random way by default, so the task is placed
    # on an arbitrary instance in your cluster, hence sometimes the instance
    # did not have enough resources while other was empty.
    # This argument and the provided values ensure the tasks are
    # evenly "spread" across all "instanceId"s.
    placement_strategy=[
        {"type": "spread", "field": "instanceId"},
    ],
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
    execution_timeout=datetime.timedelta(minutes=10),
)

# downloading_task >> comparing_task
downloading_task >> comparing_task
