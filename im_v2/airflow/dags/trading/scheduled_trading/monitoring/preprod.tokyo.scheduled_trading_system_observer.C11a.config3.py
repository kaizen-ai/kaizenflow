import copy
import datetime
import os

import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
import airflow_utils.telegram.operator as aiutteop
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).

_STAGE = aiutmisc.get_stage_from_filename(_FILENAME)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
# _USERNAME = "juraj4"
_USERNAME = "8288"

_DAG_ID = aiutmisc.get_dag_id_from_filename(_FILENAME)
_DAG_BUILDER = "C11a"
_TAGS = {"C11a": "config3"}
_DAG_BUILD_CTOR_SUFFIX = {"C11a": ""}
_DAG_REPO = {"C11a": "lemonade"}
_DAG_DESCRIPTION = "Scheduled trading system observer"
_SCHEDULE = "*/5 * * * *"

# Currently use a separate task definition to run the DAG from the branch.
# Prod/preprod stages require an empty user name, i.e. an empty string.
# _ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, True, _USERNAME)
_ECS_TASK_DEFINITION = f"cmamp-test-{_USERNAME}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": 0,
    "email": "",
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": False,
    "owner": "airflow",
}


def get_pnl_start_date(date):
    """
    Utility function to align a date to 5 min grid.
    """
    if datetime.datetime.utcnow().time() <= date.time():
        date -= datetime.timedelta(days=1)
    return date


def get_pnl_end_date(date):
    """
    Utility function to align a date to 5 min grid.
    """
    if datetime.datetime.utcnow().time() > date.time():
        date += datetime.timedelta(days=1, minutes=-5)
    else:
        date -= datetime.timedelta(minutes=5)
    return date


# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=2,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    user_defined_filters={
        "get_pnl_start_date": get_pnl_start_date,  # Macro can also be a function
        "get_pnl_end_date": get_pnl_end_date,
    },
    user_defined_macros={
        "date": "{{ data_interval_end if 'scheduled' in run_id else macros.datetime.now() }}",
    },
    start_date=datetime.datetime(2022, 8, 1, 0, 0, 0),
    tags=[_STAGE],
)


efs_mount = "{{ var.value.efs_mount_tokyo }}"
# We use data_interval_end because the DAG is supposed to start in real-time.
# dag_run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
# This refers to the system run mode.
dag_run_mode = "scheduled"
start_timestamp = "{{ macros.datetime.today().replace(hour=11, minute=30, second=0, microsecond=0) | \
                    get_pnl_start_date | ts_nodash | replace('T', '_') }}"
end_timestamp = "{{ macros.datetime.today().replace(hour=17, minute=30, second=0, microsecond=0) | \
                    get_pnl_end_date | ts_nodash | replace('T', '_') }}"
# run_date = "{{ data_interval_end.date() | string | replace('-', '') }}"
run_mode_for_log = "{{ '' if 'scheduled' in run_id else '.manual' }}"

pnl_observer_cmd = [
    "mkdir /.dockerenv",
    "&&",
    "invoke run_master_system_observer_notebook",
    "--dag-builder-ctor-as-str '{}'",
    "--prod-data-root-dir '{}'",
    "--run-mode 'prod'",
    "--notebook-name 'Master_PnL_real_time_observer'",
    "--tag {}",
    "--mark-as-last-5minute-run",
]

start_task = DummyOperator(task_id="start_dag", dag=dag)
end_task = DummyOperator(task_id="end_system_tasks", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)


tag = _TAGS[_DAG_BUILDER]

log_base_dir = f"{efs_mount}/{_STAGE}/system_reconciliation/"

curr_pnl_observer_cmd = copy.deepcopy(pnl_observer_cmd)

dag_builder_ctor_as_str = (
    "dataflow_{}.pipelines.{}.{}_pipeline{}.{}_DagBuilder{}".format(
        _DAG_REPO[_DAG_BUILDER],
        _DAG_BUILDER[:3],
        _DAG_BUILDER,
        _DAG_BUILD_CTOR_SUFFIX[_DAG_BUILDER],
        _DAG_BUILDER,
        _DAG_BUILD_CTOR_SUFFIX[_DAG_BUILDER],
    )
)

curr_pnl_observer_cmd[3] = curr_pnl_observer_cmd[3].format(
    dag_builder_ctor_as_str
)
curr_pnl_observer_cmd[4] = curr_pnl_observer_cmd[4].format(log_base_dir)
curr_pnl_observer_cmd[7] = curr_pnl_observer_cmd[7].format(tag)

pnl_observer_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"{_DAG_BUILDER}.pnl_observer",
    curr_pnl_observer_cmd,
    _ECS_TASK_DEFINITION,
    1024,
    4096,
    region=aiutecop.ASIA_REGION,
)

start_task >> pnl_observer_task >> end_task >> end_dag

telegram_notification_task = aiutteop.get_telegram_operator(
    dag, _STAGE, "trading", _DAG_ID, "{{ run_id }}"
)
end_task >> telegram_notification_task >> end_dag