"""
This DAG is used to download realtime data to the IM database via websockets.

In case of bid/ask data, a parallel resampling task runs which resamples
raw data to 1 minute on the fly.
"""

import datetime
import os

import airflow
import airflow_utils.datapull.datapull_utils as adauddu
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
import airflow_utils.telegram.operator as aiutteop
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# Extract the components from the filename.
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# Deployment type, if the task should be run via fargate (serverless execution)
# or EC2 (machines deployed in our auto-scaling group)
_LAUNCH_TYPE = "fargate"

_UNIVERSE = {
    "binance": "v7.3",
    "binanceus": "v7.3",
    "okx": "v7.6",
    "kraken": "v7.4",
    "cryptocom": "v8.1",
}
_DOWNLOAD_JOBS = [
    ("binance", "spot"),
    ("okx", "spot"),
    ("cryptocom", "futures"),
]

# How many levels deep in to order book
# to downlaod per iteration per symbol
components_dict[
    "bid_ask_depth"
] = "{{ var.value.websocket_download_bid_ask_depth }}"
# These values are changed dynamically based on DAG purpose and nature
# of the downloaded data
components_dict["download_mode"] = "realtime"
components_dict["action_tag"] = "downloaded_200ms"
components_dict["data_format"] = "postgres"
# The value is implicit since this is an Airflow DAG.
components_dict["downloading_entity"] = "airflow"
_DAG_DESCRIPTION = "Realtime data download and resampling."
# Specify when/how often to execute the DAG.
_SCHEDULE = "0 * * * *"

_ECS_TASK_DEFINITION = aiutecop.get_task_definition(
    components_dict["stage"], False, _USERNAME
)

# Pass default parameters for the DAG.
default_args = {
    "retries": 1 if components_dict["stage"] in ["prod", "preprod"] else 0,
    "retry_delay": 0,
    "email": [Variable.get(f'{components_dict["stage"]}_notification_email')],
    "email_on_failure": True
    if components_dict["stage"] in ["prod", "preprod"]
    else False,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=components_dict["dag_id"],
    description=_DAG_DESCRIPTION,
    max_active_runs=3,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    tags=[components_dict["stage"]],
)

components_dict[
    "start_time"
] = "{{ macros.datetime.now(dag.timezone).replace(second=0, microsecond=0) + macros.timedelta(minutes=var.value.rt_data_download_standby_min | int, seconds=10) }}"
components_dict[
    "stop_time"
] = "{{ data_interval_end + macros.timedelta(minutes=(var.value.rt_data_download_run_for_min | int) + (var.value.rt_data_download_standby_min | int) + 2) }}"

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for exchange_id, contract_type in _DOWNLOAD_JOBS:
    # Overriding the default values according to the download job.
    components_dict["exchange_id"] = exchange_id
    components_dict["contract_type"] = contract_type
    components_dict["universe"] = _UNIVERSE[exchange_id]
    components_dict["db_stage"] = components_dict["stage"]
    # Get the download command for the websocket data.
    download_command = adauddu.get_command_from_dag_name(components_dict)
    # Define the task for downloading the data.
    downloading_task = aiutecop.get_ecs_run_task_operator(
        dag,
        components_dict["stage"],
        f"{components_dict['download_mode']}.download.{components_dict['vendor']}.{exchange_id}.{components_dict['data_type']}.{contract_type}",
        download_command,
        _ECS_TASK_DEFINITION,
        256,
        1024,
        assign_public_ip=True,
    )
    # Define the sequence of execution of task.
    start_task >> downloading_task >> end_task >> end_dag

if components_dict["stage"] != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag,
        components_dict["stage"],
        "datapull",
        components_dict["dag_id"],
        "{{ run_id }}",
    )
    end_task >> telegram_notification_task >> end_dag
