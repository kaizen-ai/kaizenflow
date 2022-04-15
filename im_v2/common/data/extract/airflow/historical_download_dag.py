"""
This is a utility DAG to conveniently download bigger chunks of historical
data using manual dag trigger, example parameters provided on manual run:
{
     "exchange": "ftx",
     "vendor": "talos",
     "start_timestamp": "2019-01-01T00:00:00+00:00",
     "end_timestamp": "2019-01-10T00:00:00+00:00",
     "snapshot_name": "test",
     "universe": "v1"
}

Import as:

import im_v2.common.data.extract.airflow.historical_download_dag as imvcdeahdd
"""

import datetime
from pickle import FALSE

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to prod
# in one line (in best case scenario).
_STAGE = "test"
_EXCHANGE = "{{ dag_run.conf['exchange'] }}"
# Provide the vendor in lowercase e.g. CCXT -> ccxt.
_VENDOR = "{{ dag_run.conf['vendor'] }}"
_UNIVERSE = "{{ dag_run.conf['universe'] }}"

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
snapshot_folder = "{{ dag_run.conf['snapshot_name'] }}"
s3_historical_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_historical_data_folder')}/{_VENDOR}/{snapshot_folder}"


# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": False,
    "email_on_retry": FALSE,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=f"{_STAGE}_historical_download",
    description="Download historical data for specified time frame from chosen vendor and exchange",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="15 0 * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 2, 10, 0, 15, 0),
)

start_timestamp = "{{ dag_run.conf['start_timestamp'] }}"
end_timestamp = "{{ dag_run.conf['end_timestamp'] }}"

download_command = [
    f"/app/im_v2/{_VENDOR}/data/extract/download_historical_data.py",
    f"--start_timestamp '{start_timestamp}'",
    f"--end_timestamp '{end_timestamp}'",
    f"--exchange_id '{_EXCHANGE}'",
    f"--universe '{_UNIVERSE}'",
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
)

downloading_task
