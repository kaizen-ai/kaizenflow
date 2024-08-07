"""
Resample bid/ask data from 200ms to 1min intervals for Binance and OKX.
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

# Provide list of dicts in format:
# {
#   "src_signature": 'periodic_daily...',
#   "dst_signature": 'periodic_faily...'
# }
_RESAMPLING_JOBS = [
    {
        "src_signature": "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7.ccxt.binance.v1_0_0",
        "dst_signature": "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7.ccxt.binance.v2_0_0",
    },
    {
        "src_signature": "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_6.ccxt.okx.v1_0_0",
        "dst_signature": "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_6.ccxt.okx.v2_0_0",
    },
]
_DAG_DESCRIPTION = "Resample bid/ask data"
s3_bucket = f"{components_dict['stage']}_s3_data_bucket"
s3_path = f"s3://{Variable.get(s3_bucket)}"
components_dict["src_s3_path"] = s3_path
components_dict["dst_s3_path"] = s3_path
# Specify when/how often to execute the DAG.
_SCHEDULE = "20 4 * * *"

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
    start_date=datetime.datetime(2023, 11, 1, 0, 0, 0),
    tags=[components_dict["stage"]],
)

components_dict[
    "start_timestamp"
] = "{{ data_interval_start.replace(hour=0, minute=0, second=0) }}"
components_dict[
    "end_timestamp"
] = "{{ data_interval_end.replace(hour=0, minute=0, second=0) - macros.timedelta(microseconds=1) }}"
components_dict["bid_ask_levels"] = 1
components_dict["assert_all_resampled"] = ""

start_dag = DummyOperator(task_id="start_dag", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for job in _RESAMPLING_JOBS:
    components_dict["src_signature"] = job["src_signature"]
    components_dict["dst_signature"] = job["dst_signature"]
    resample_command = adauddu.get_command_from_dag_name(components_dict)

    resampling_task = aiutecop.get_ecs_run_task_operator(
        dag,
        components_dict["stage"],
        f"resample.{job['dst_signature']}",
        resample_command,
        _ECS_TASK_DEFINITION,
        2048,
        8192,
    )

start_dag >> resampling_task >> end_dag
