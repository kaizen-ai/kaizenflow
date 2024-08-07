# This is a utility DAG to conduct QA on real time data download
# The task compares the downloaded data with the contents of
# of the database, to confirm a match or show discrepancies.

# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval`` parameter.

# This DAG's configuration deploys tasks to AWS Fargate to offload the EC2s
# mainly utilized for rt download
import datetime
import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
from airflow.operators.dummy_operator import DummyOperator
import airflow_utils.telegram.operator as aiutteop
from airflow.models import Variable
import airflow_utils.aws.connection as aiutawco
import copy
import os

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to 
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
#_STAGE = _FILENAME.split(".")[0]
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]
_REGION = aiutecop.ASIA_REGION if components_dict["location"] == "tokyo" else aiutecop._EUROPE_REGION

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# Deployment type, if the task should be run via fargate (serverless execution)
# or EC2 (machines deployed in our auto-scaling group)
_LAUNCH_TYPE = "fargate"

_DAG_ID = components_dict["dag_id"]
# List of dicts to specify parameters for each reconciliation jobs.
# "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
_QA_JOBS = [
    {
        "dataset_signature1": "realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7_3.ccxt.binance.v1_0_0",
        "dataset_signature2": "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_3.ccxt.binance.v1_0_0",
        "add_invoke_params": []
    },
    #{
    #    "dataset_signature1": "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v7.ccxt.binance.v1_0_0",
    #    "dataset_signature2":  "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0",
    #    "add_invoke_params": ["--bid-ask-accuracy {{ var.value.bid_ask_qa_acc_thresh }}"]
    #}
    {
        "dataset_signature1": "realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7_6.ccxt.okx.v1_0_0",
        "dataset_signature2": "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_6.ccxt.okx.v1_0_0",
        "add_invoke_params": []
    },   
]
# Shared location to store the reconciliaiton notebook into
_QA_NB_DST_DIR = os.path.join("{{ var.value.efs_mount }}", _STAGE, "data_qa", "periodic_daily")
_DAG_DESCRIPTION = "Daily data QA. Run QA notebook and publish results" \
                    + " to a shared EFS."
_SCHEDULE = "@daily"

_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f'{_STAGE}_notification_email')],
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": False,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2023, 3, 22, 0, 0, 0),
)

invoke_cmd = [
    "invoke run_cross_dataset_qa_notebook",
    f"--stage '{_STAGE}'",
    "--start-timestamp '{{ data_interval_start.replace(hour=0, minute=0, second=0) }}'",
    "--end-timestamp '{{ data_interval_end.replace(hour=0, minute=0, second=0) - macros.timedelta(minutes=1) }}'",
    "--dataset-signature1 '{}'",
    "--dataset-signature2 '{}'",
    "--aws-profile 'ck'",
    f"--base-dst-dir '{_QA_NB_DST_DIR}'",
]

start_comparison = DummyOperator(task_id='start_comparison', dag=dag)
end_comparison = DummyOperator(task_id='end_comparison', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

for job in _QA_JOBS:
    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash cmd list so we can reformat params on each iteration.
    curr_invoke_cmd = copy.deepcopy(invoke_cmd)
    curr_invoke_cmd[4] = curr_invoke_cmd[4].format(job["dataset_signature1"])
    curr_invoke_cmd[5] = curr_invoke_cmd[5].format(job["dataset_signature2"])
    for param in job["add_invoke_params"]:
        curr_invoke_cmd.append(param)
        
    curr_invoke_cmd = ["mkdir /.dockerenv", "&&"] + curr_invoke_cmd
    
    comparing_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"data_qa.{job['dataset_signature1']}",
        curr_invoke_cmd,
        _ECS_TASK_DEFINITION,
        512, 
        2048
    )
    
    start_comparison >> comparing_task >> end_comparison >> end_dag
    
if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(dag, _STAGE, "datapull", _DAG_ID, "{{ run_id }}")
    end_comparison >> telegram_notification_task >> end_dag