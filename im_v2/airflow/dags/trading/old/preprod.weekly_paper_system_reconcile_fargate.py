# This is a utility DAG to conduct QA on real time data download
# DAG task downloads data for last N minutes in one batch

import copy
import datetime
import os

import airflow
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
_STAGE = aiutmisc.get_stage_from_filename(_FILENAME)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# Deployment type, if the task should be run via fargate (serverless execution)
# or EC2 (machines deployed in our auto-scaling group)
_LAUNCH_TYPE = "fargate"

_DAG_ID = aiutmisc.get_dag_id_from_filename(_FILENAME)
# _DAG_BUILDERS = ["C1b", "C3a", "C8b", "C5b"]
_DAG_BUILDERS = ["C3a"]
_LIVENESS = "CANDIDATE"
_INSTANCE_TYPE = "PROD"
_VERBOSITY = "DEBUG"
_EXCHANGE = "binance"
_SYSTEM_STAGE = "preprod"
_ACCOUNT_TYPE = "trading"
_SECRET_ID = 3
_DAG_DESCRIPTION = "Weekly system renconciliation"
# Run duration (specified in hours so it can be propagated to both container
# ans Airflow 'cron' schedule).
_SYSTEM_RUN_MODE = "paper_trading"
_RUN_DURATION = 24
_RUN_DURATION_IN_SECS = int(_RUN_DURATION) * 3600
# _SCHEDULE = "0 5 * * *" from
_SCHEDULE = "0 7 * * MON"
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, True, _USERNAME)
# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "retry_delay": 0,
    "email": [
        Variable.get(f"{_STAGE}_notification_email"),
        "g.pomazkin@crypto-kaizen.com",
    ],
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": True,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=2,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    user_defined_filters={
        "align_to_5min_grid": aiutmisc.align_to_5min_grid,  # Macro can also be a function
    },
    start_date=datetime.datetime(2022, 8, 1, 0, 0, 0),
    tags=[_STAGE],
)


efs_mount = "{{ var.value.efs_mount }}"
# We use data_interval_end because the DAG is supposed to start in real-time.
dag_run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
start_timestamp = "{{ data_interval_start | ts_nodash | replace('T', '_') }}"
end_timestamp = "{{ data_interval_end | ts_nodash | replace('T', '_') }}"
run_date = "{{ dag_run.get_task_instance('start_dag').start_date.date() | string | replace('-', '') }}"
run_mode_for_log = "{{ '' if 'scheduled' in run_id else '.manual' }}"
log_dir_specifier = f"{start_timestamp}.{end_timestamp}"
system_reconcile_cmd = [
    # This is a hack to pass hserver.is_inside_docker()
    #  because when ran in ECS it is not present there
    #  by default.
    "mkdir /.dockerenv",
    "&&",
    "invoke reconcile_run_multiday_notebook",
    "--dag-builder-name '{}'",
    "--dst-root-dir '{}'",
    f"--run-mode '{_SYSTEM_RUN_MODE}'",
    f"--start-timestamp-as-str {start_timestamp}",
    f"--end-timestamp-as-str {end_timestamp}",
    "--html-notebook-tag 'last_week'",
]

start_task = DummyOperator(task_id="start_dag", dag=dag)
end_task = DummyOperator(task_id="end_system_tasks", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for dag_builder in _DAG_BUILDERS:
    curr_reconcile_command = copy.deepcopy(system_reconcile_cmd)
    curr_reconcile_command[3] = curr_reconcile_command[3].format(dag_builder)
    reconc_log_base_dir = f"{efs_mount}/{_STAGE}/prod_reconciliation/"
    curr_reconcile_command[4] = curr_reconcile_command[4].format(
        reconc_log_base_dir
    )

    system_reconcile_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"{dag_builder}.system_reconcile",
        curr_reconcile_command,
        _ECS_TASK_DEFINITION,
        2048,
        8192,
    )

    start_task >> system_reconcile_task >> end_task >> end_dag

if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag, _STAGE, "datapull", _DAG_ID, "{{ run_id }}"
    )
    end_task >> telegram_notification_task >> end_dag
