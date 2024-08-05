"""
Airflow DAG to download bulk OHLCV data from CCXT exchanges.
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

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

components_dict["contract_type"] = "futures"
_DOWNLOAD_JOBS = [
    ("binance", "v7.3"),
    ("binance", "all"),
    ("cryptocom", "v8.1"),
    ("okx", "v8"),
]
# These values are changed dynamically based on DAG purpose and nature
#  of the downloaded data
components_dict["download_mode"] = "periodic_daily"
components_dict["action_tag"] = "downloaded_1min"
components_dict["data_format"] = "parquet"
s3_bucket = f"{components_dict['stage']}_s3_data_bucket"
components_dict["s3_path"] = f"s3://{Variable.get(s3_bucket)}"
# The value is implicit since this is an Airflow DAG.
components_dict["downloading_entity"] = "airflow"
_DAG_DESCRIPTION = f"Daily {components_dict['data_type']} data download."
# Specify when/how often to execute the DAG.
_SCHEDULE = "20 1 * * *"
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(
    components_dict["stage"], False, _USERNAME
)

# Pass default parameters for the DAG.
default_args = {
    "retries": 1 if components_dict["stage"] in ["prod", "preprod"] else 0,
    "email": [Variable.get(f"{components_dict['stage']}_notification_email")],
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
    start_date=datetime.datetime(2023, 3, 20, 0, 0, 0),
    tags=[components_dict["stage"]],
)

components_dict[
    "end_timestamp"
] = "{{ data_interval_end.replace(hour=0, minute=0, second=0) - macros.timedelta(minutes=1) }}"
components_dict[
    "start_timestamp"
] = "{{ data_interval_start.replace(hour=0, minute=0, second=0) }}"

start_task = DummyOperator(task_id="start_dag", dag=dag)
end_download = DummyOperator(task_id="end_download", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for exchange_id, universe in _DOWNLOAD_JOBS:
    components_dict["exchange_id"] = exchange_id
    components_dict["universe"] = universe

    download_command = adauddu.get_download_bulk_data_command(components_dict)

    downloading_task = aiutecop.get_ecs_run_task_operator(
        dag,
        components_dict["stage"],
        f"download.{components_dict['download_mode']}.{components_dict['vendor']}.{components_dict['exchange_id']}.{components_dict['contract_type']}.{components_dict['universe']}",
        download_command,
        _ECS_TASK_DEFINITION,
        256,
        512,
        assign_public_ip=True,
    )

    start_task >> downloading_task >> end_download >> end_dag

if components_dict["stage"] != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag,
        components_dict["stage"],
        "datapull",
        components_dict["dag_id"],
        "{{ run_id }}",
    )
    end_download >> telegram_notification_task >> end_dag
