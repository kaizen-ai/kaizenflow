# This is a utility DAG to conduct QA on real time data download
# The task analyzes the downloaded data stored inside an RDS database with the contents,
# to confirm expected attributes of the data

# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval` parameter.

# This DAG's configuration deploys tasks to AWS Fargate to offload the EC2s
# mainly utilized for rt download
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
# _STAGE = _FILENAME.split(".")[0]
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# List of dataset signatures to perform QA on. i.e.:
# "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
_QA_JOBS = [
    "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_3.ccxt.binance.v1_0_0",
    "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v7_3.ccxt.binance.v1_0_0",
    # "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_6.ccxt.okx.v1_0_0",
    # "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0",
]
# Shared location to store the reconciliaiton notebook into
components_dict["base-dst-dir"] = os.path.join(
    "{{ var.value.efs_mount }}",
    components_dict["stage"],
    components_dict["purpose"],
    components_dict["period"],
)
_DAG_DESCRIPTION = (
    "Data QA. Run QA notebook and publish results" + " to a shared EFS."
)
_SCHEDULE = "5-59/10 * * * *"

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
    start_date=datetime.datetime(2023, 1, 24, 0, 0, 0),
    tags=[components_dict["stage"]],
)

components_dict[
    "start-timestamp"
] = "{{ data_interval_start.replace(second=0) - macros.timedelta(minutes=5) }}"
components_dict[
    "end-timestamp"
] = "{{ data_interval_end.replace(second=0) - macros.timedelta(minutes=5) }}"

start_comparison = DummyOperator(task_id="start_comparison", dag=dag)
end_comparison = DummyOperator(task_id="end_comparison", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for dataset_signature in _QA_JOBS:
    components_dict["dataset-signature"] = dataset_signature
    data_qa_command = adauddu.get_run_single_dataset_qa_command(components_dict)
    comparing_task = aiutecop.get_ecs_run_task_operator(
        dag,
        components_dict["stage"],
        f"data_qa.{dataset_signature}",
        data_qa_command,
        _ECS_TASK_DEFINITION,
        512,
        2048,
    )

    start_comparison >> comparing_task >> end_comparison >> end_dag

if components_dict["stage"] != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag,
        components_dict["stage"],
        "datapull",
        components_dict["dag_id"],
        "{{ run_id }}",
    )
    end_comparison >> telegram_notification_task >> end_dag
