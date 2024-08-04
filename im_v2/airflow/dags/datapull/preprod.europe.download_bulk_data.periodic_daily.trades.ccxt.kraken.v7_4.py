"""
Airflow DAG to download Kraken v7.4 trades data from CCXT.
"""

import datetime
import os

import airflow
import airflow_utils.datapull.datapull_utils as adauddu
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

components_dict["contract_type"] = "spot"
# These values are changed dynamically based on DAG purpose and nature
#  of the downloaded data
components_dict["download_mode"] = "periodic_daily"
components_dict["action_tag"] = "downloaded_all"
components_dict["data_format"] = "parquet"
s3_bucket = f"{components_dict['stage']}_s3_data_bucket"
components_dict["s3_path"] = f"s3://{Variable.get(s3_bucket)}"
# The value is implicit since this is an Airflow DAG.
components_dict["downloading_entity"] = "airflow"
_DAG_DESCRIPTION = (
    f'Daily {components_dict["data_type"]} data download, contracts:'
    + f'{components_dict["contract_type"]}, using {components_dict["vendor"]} from {components_dict["exchange"]}.'
)
# Specify when/how often to execute the DAG.
_SCHEDULE = "20 2 * * *"
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(
    components_dict["stage"], False, _USERNAME
)
# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f'{components_dict["stage"]}_notification_email')],
    "email_on_failure": True
    if components_dict["stage"] in ["prod", "preprod"]
    else False,
    "email_on_retry": False,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=components_dict["dag_id"],
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2023, 3, 15, 0, 0, 0),
    tags=[components_dict["stage"]],
)

components_dict[
    "start_timestamp"
] = "{{data_interval_start.replace(hour=23, minute=59, second=59)}}"
components_dict[
    "end_timestamp"
] = "{{data_interval_start.replace(hour=0, minute=0, second=0)}}"
components_dict["assert_on_missing_data"] = ""

start_task = DummyOperator(task_id="start_dag", dag=dag)
end_download = DummyOperator(task_id="end_dag", dag=dag)

components_dict["exchange_id"] = components_dict["exchange"]

download_command = adauddu.get_command_from_dag_name(components_dict)

downloading_task = aiutecop.get_ecs_run_task_operator(
    dag,
    components_dict["stage"],
    f"download.{components_dict['download_mode']}.{components_dict['vendor']}.{components_dict['exchange_id']}.{components_dict['contract_type']}",
    download_command,
    _ECS_TASK_DEFINITION,
    2048,
    8192,
    assign_public_ip=True,
)

start_task >> downloading_task >> end_download
