"""
Import as:

import im_v2.ccxt.data.extract.airflow.prod_daily_data_reconciliation_dag as imvcdeapddrd
"""

import datetime

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to prod
# in one line (in best case scenario)
_STAGE = "prod"
_EXCHANGE = "binance"
# E.g. DB table ccxt_ohlcv -> has an equivalent for testing ccxt_ohlcv_test
# but production is ccxt_ohlcv.
_TABLE_SUFFIX = "_" + _STAGE if _STAGE == "test" else ""

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
s3_historical_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_historical_data_folder')}/"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get("notification_email")],
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
    "/app/im_v2/ccxt/data/extract/download_historical_data.py",
    "--start_timestamp '{{ execution_date - macros.timedelta(1) - macros.timedelta(minutes=15) }}'",
    "--end_timestamp '{{ execution_date - macros.timedelta(minutes=15) }}'",
    f"--exchange_id '{_EXCHANGE}'",
    "--universe 'v03'",
    "--aws_profile 'ck'",
    f"--s3_path '{s3_historical_data_path}'",
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
                "environment": [
                    {
                        "name": "ENABLE_DIND",
                        "value": "0",
                    }
                ],
            }
        ]
    },
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
)
compare_command = [
    "/app/im_v2/ccxt/data/extract/compare_realtime_and_historical.py",
    "--db_stage 'dev'",
    "--start_timestamp {{ execution_date - macros.timedelta(1) - macros.timedelta(minutes=15) }}",
    "--end_timestamp {{ execution_date - macros.timedelta(minutes=15) }}",
    f"--exchange_id '{_EXCHANGE}'",
    f"--db_table 'ccxt_ohlcv'",
    "--aws_profile 'ck'",
    f"--s3_path '{s3_historical_data_path}'",
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
                "environment": [
                    {
                        "name": "ENABLE_DIND",
                        "value": "0",
                    }
                ],
            }
        ]
    },
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
)
# Execute the DAG.
downloading_task >> comparing_task