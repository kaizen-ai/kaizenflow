"""
This DAG is used to download realtime data to the ohlcv tables in the database.

Import as:

import im_v2.common.data.extract.airflow.prod_rt_dag as imvcdeaprd
"""

import copy
import datetime
from itertools import product

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

_STAGE = "prod"
_EXCHANGES = ["binance", "ftx"]
_PROVIDERS = ["ccxt", "talos"]
_UNIVERSES = {"ccxt": "v3", "talos": "v1"}

# E.g. DB table ccxt_ohlcv -> has an equivalent for testing ccxt_ohlcv_test
# but production is ccxt_ohlcv.
_TABLE_SUFFIX = f"_{_STAGE}" if _STAGE == "test" else ""

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": True,
    "owner": "airflow",
}

# Create a command, leave values to be parametrized.
bash_command = [
    "/app/im_v2/{}/data/extract/download_realtime_for_one_exchange.py",
    "--start_timestamp {{ execution_date - macros.timedelta(minutes=5) }}",
    "--end_timestamp {{ execution_date }}",
    "--exchange_id '{}'",
    "--universe '{}'",
    "--db_stage 'dev'",
    "--db_table '{}_ohlcv{}'",
    "--aws_profile 'ck'",
    "--s3_path 's3://{}/{}/{}'",
]

# Create a DAG.
dag = airflow.DAG(
    dag_id=f"{_STAGE}_realtime_dag",
    description=f"Realtime download of {_PROVIDERS} OHLCV data from {_EXCHANGES}",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 3, 1, 0, 0, 0),
)

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

for provider, exchange in product(_PROVIDERS, _EXCHANGES):

    # TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(bash_command)
    curr_bash_command[0] = curr_bash_command[0].format(provider)
    curr_bash_command[3] = curr_bash_command[3].format(exchange)
    curr_bash_command[4] = curr_bash_command[4].format(_UNIVERSES[provider])
    curr_bash_command[-3] = curr_bash_command[-3].format(provider, _TABLE_SUFFIX)
    curr_bash_command[-1] = curr_bash_command[-1].format(
        Variable.get(f"{_STAGE}_s3_data_bucket"),
        Variable.get("s3_realtime_data_folder"),
        provider,
    )

    container_suffix = f"-{_STAGE}" if _STAGE == "test" else ""
    container_name = f"cmamp{container_suffix}"

    downloading_task = ECSOperator(
        task_id=f"rt_{provider}_{exchange}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="EC2",
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": curr_bash_command,
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
        execution_timeout=datetime.timedelta(minutes=3),
    )
    # Define the sequence of execution of task.
    start_task >> downloading_task >> end_task
