"""
This DAG is used to download resampled realtime data to the IM database via
websockets.
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

components_dict["universe"] = components_dict["universe"].replace(".", "_")
# Generate the dag_signature based on data type, vendor and exchange.
_RESAMPLING_JOBS = [
    f'realtime.airflow.downloaded_200ms.postgres.{components_dict["data_type"]}.futures.{components_dict["universe"]}.{components_dict["vendor"]}.{components_dict["exchange"]}.v1_0_0'
]

# These values are changed dynamically based on DAG purpose and nature
#  of the downloaded data
_DAG_DESCRIPTION = "Realtime data resampling."
# Specify when/how often to execute the DAG.
_SCHEDULE = "0 */2 * * *"

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

components_dict["db_stage"] = components_dict["stage"]
components_dict[
    "start_ts"
] = "{{ macros.datetime.now(dag.timezone).replace(second=0, microsecond=0) + macros.timedelta(minutes=(var.value.rt_data_download_standby_min | int), milliseconds=200) }}"
components_dict[
    "end_ts"
] = "{{ data_interval_end + macros.timedelta(minutes=(var.value.rt_data_download_run_for_min | int) + var.value.rt_data_download_standby_min | int) }}"
components_dict["resample_freq"] = "1T"

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for dag_signature in _RESAMPLING_JOBS:
    components_dict["dag_signature"] = dag_signature
    resample_command = adauddu.get_command_from_dag_name(components_dict)

    resampling_task = aiutecop.get_ecs_run_task_operator(
        dag,
        components_dict["stage"],
        f"{dag_signature}",
        resample_command,
        _ECS_TASK_DEFINITION,
        1024,
        6144,
        assign_public_ip=True,
    )

    # Define the sequence of execution of task.
    start_task >> resampling_task >> end_task >> end_dag

if components_dict["stage"] != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag,
        components_dict["stage"],
        "datapull",
        components_dict["dag_id"],
        "{{ run_id }}",
    )
    end_task >> telegram_notification_task >> end_dag
