# This is a utility DAG to conduct QA on real time data download
# The first DAG task download data for last N minutes in one batch
# The second task compares the downloaded data with the contents of
# of the database, to confirm a match or show discrepancies
# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval`` parameter.
# Make sure to set the correct timedelta for `start_timestamp` cmd line
# argument in all variables representing bash commands.
import copy
import datetime
from itertools import product

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to prod
# in one line (in best case scenario).
_STAGE = "test"
_DAG_ID = f"{_STAGE}_daily_data_reconciliation_multi_dag"
_EXCHANGES = ["ftx", "binance"]
_PROVIDERS = ["ccxt"]
_UNIVERSES = {"ccxt": "v3"}

# E.g. DB table ccxt_ohlcv -> has an equivalent for testing ccxt_ohlcv_test
# but production is ccxt_ohlcv.
_TABLE_SUFFIX = f"_{_STAGE}" if _STAGE == "test" else ""
# Used for container overrides inside DAG task definition.
CONTAINER_SUFFIX = f"-{_STAGE}" if _STAGE == "test" else ""
CONTAINER_NAME = f"cmamp{CONTAINER_SUFFIX}"

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
s3_daily_staged_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_daily_staged_data_folder')}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": True,
    "email_on_retry": True,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description="Download past day data and confirm correctness and completeness of RT data.",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 4, 25, 0, 15, 0),
)

download_command = [
    "/app/im_v2/{}/data/extract/download_historical_data.py",
    "--end_timestamp '{{ execution_date }}'",
    "--start_timestamp '{{ execution_date - macros.timedelta(minutes=30) }}'",
    "--exchange_id '{}'",
    "--universe '{}'",
    "--aws_profile 'ck'",
    "--s3_path '{}/{}'",
]

start_task = DummyOperator(task_id="start_download", dag=dag)
end_download = DummyOperator(task_id="end_download", dag=dag)

# Used for container overrides in task definition
container_suffix = f"-{_STAGE}" if _STAGE == "test" else ""
container_name = f"cmamp{container_suffix}"

for provider, exchange in product(_PROVIDERS, _EXCHANGES):

    # TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(download_command)
    curr_bash_command[0] = curr_bash_command[0].format(provider)
    curr_bash_command[3] = curr_bash_command[3].format(exchange)
    curr_bash_command[4] = curr_bash_command[4].format(_UNIVERSES[provider])
    curr_bash_command[-1] = curr_bash_command[-1].format(
        s3_daily_staged_data_path, provider
    )

    downloading_task = ECSOperator(
        task_id=f"download_{provider}_{exchange}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="EC2",
        overrides={
            "containerOverrides": [
                {
                    "name": CONTAINER_NAME,
                    "command": curr_bash_command,
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
    start_task >> downloading_task >> end_download

compare_command = [
    "/app/im_v2/{}/data/extract/compare_realtime_and_historical.py",
    "--end_timestamp '{{ execution_date }}'",
    "--start_timestamp '{{ execution_date - macros.timedelta(minutes=30) }}'",
    "--db_stage 'dev'",
    "--exchange_id '{}'",
    "--db_table '{}_ohlcv{}'",
    "--aws_profile 'ck'",
    "--s3_path '{}/{}'",
]

for provider, exchange in product(_PROVIDERS, _EXCHANGES):

    # TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(compare_command)
    curr_bash_command[0] = curr_bash_command[0].format(provider)
    curr_bash_command[4] = curr_bash_command[4].format(exchange)
    curr_bash_command[5] = curr_bash_command[5].format(provider, _TABLE_SUFFIX)
    curr_bash_command[-1] = curr_bash_command[-1].format(
        s3_daily_staged_data_path, provider
    )

    comparing_task = ECSOperator(
        task_id=f"compare_{provider}_{exchange}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="EC2",
        # Make sure to run even if some of
        # the upstream tasks failed.
        trigger_rule="all_done",
        overrides={
            "containerOverrides": [
                {
                    "name": CONTAINER_NAME,
                    "command": curr_bash_command,
                }
            ]
        },
        placement_strategy=[
            {"type": "spread", "field": "instanceId"},
        ],
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
        execution_timeout=datetime.timedelta(minutes=10),
    )
    end_download >> comparing_task
