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
from airflow.models.param import Param
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

_DAG_ID = aiutmisc.get_dag_id_from_filename(_FILENAME)
_DAG_BUILDERS = ["C12a"]
# _DAG_BUILDERS = ["C12a"]
_DAG_BUILD_CTOR_SUFFIX = {
    "C1b": "",
    "C3a": "_tmp",
    "C8b": "_tmp",
    "C5b": "",
    "C12a": "",
}
_DAG_REPO = {
    "C1b": "orange",
    "C3a": "orange",
    "C8b": "orange",
    "C5b": "lemonade",
    "C12a": "lemonade",
}
_DAG_CONFIG_PARAMS = {
    "C12a": {
        "run": [
            '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"))\'',
            '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","kwargs"),({"target_dollar_risk_per_name": float(0.4), "prediction_abs_threshold": float(0.3)})\'',
            '--set_config_value \'("dag_config", "resample", "transformer_kwargs", "rule"),(str("3T"))\'',
            '--set_config_value \'("trading_period"),(str("3T"))\'',
            '--set_config_value \'("market_data_config","days"),(pd.Timedelta(str("62T")))\'',
            '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(3))\'',
            '--set_config_value \'("portfolio_config", "pricing_method"),(str("last"))\'',
            '--set_config_value \'("market_data_config","universe_version"),(str("v7.5"))\'',
        ],
        "reconcile": [
            '--set-config-values \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","kwargs"),({"target_dollar_risk_per_name": float(0.4), "prediction_abs_threshold": float(0.3)});("dag_config", "resample", "transformer_kwargs", "rule"),(str("3T"));("trading_period"),(str("3T"));("market_data_config","days"),(pd.Timedelta(str("62T")));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(3));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_type"),(str("price@start"));("portfolio_config","pricing_method"),(str("last"));("market_data_config","universe_version"),(str("v7.5"))\''
        ],
    }
}
_DAG_BAR_DURATION_MIN = {"C1b": 5, "C3a": 5, "C8b": 5, "C5b": 5, "C12a": 3}
_LIVENESS = "CANDIDATE"
_INSTANCE_TYPE = "PROD"
_VERBOSITY = "DEBUG"
_EXCHANGE = "binance"
_SYSTEM_STAGE = "preprod"
_ACCOUNT_TYPE = "trading"
_SECRET_ID = 3
_DAG_DESCRIPTION = "Test System DAG"
# Run duration (specified in hours so it can be propagated to both container
# ans Airflow 'cron' schedule).
_SYSTEM_RUN_MODE = "paper_trading"
_RUN_DURATION = 24
_RUN_DURATION_IN_SECS = int(_RUN_DURATION) * 3600
_SCHEDULE = "0 13 * * * "
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, True, _USERNAME)

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": 0,
    "email": [Variable.get(f"{_STAGE}_trading_notification_email")],
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": True if _STAGE in ["prod", "preprod"] else False,
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
    user_defined_macros={"align_to_grid": aiutmisc.align_to_grid},
    params={
        "run_duration_in_sec": Param(
            _RUN_DURATION_IN_SECS, "How long should the run be (in seconds)"
        ),
        # "dag_start_time": Param(datetime.datetime.now(), "When the run should start")
    },
    start_date=datetime.datetime(2022, 8, 1, 0, 0, 0),
    tags=[_STAGE],
)


efs_mount = "{{ var.value.efs_mount }}"
# We use data_interval_end because the DAG is supposed to start in real-time.
dag_run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
run_date = "{{ dag_run.get_task_instance('start_dag').start_date.date() | string | replace('-', '') }}"
run_mode_for_log = "{{ '' if 'scheduled' in run_id else '.manual' }}"
system_run_cmd = [
    "/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py",
    "--strategy '{}'",
    "--dag_builder_ctor_as_str '{}'",
    f"--trade_date {run_date}",
    f"--liveness '{_LIVENESS}'",
    f"--instance_type '{_INSTANCE_TYPE}'",
    f"--exchange '{_EXCHANGE}' ",
    f"--stage '{_SYSTEM_STAGE}'",
    f"--account_type '{_ACCOUNT_TYPE}'",
    f"--secret_id '{_SECRET_ID}'",
    f"-v '{_VERBOSITY}' 2>&1",
    "--run_duration {{ params.run_duration_in_sec }}",
    f"--run_mode '{_SYSTEM_RUN_MODE}'",
    "--start_time '{}'",
    "--log_file_name  '{}/logs/log.{}.txt'",
    "--dst_dir '{}/system_log_dir.{}'",
]

system_reconcile_cmd = [
    # This is a hack to pass hserver.is_inside_docker()
    #  because when ran in ECS it is not present there
    #  by default.
    "mkdir /.dockerenv",
    "&&",
    "invoke reconcile_run_all",
    "--dag-builder-ctor-as-str '{}'",
    "--dst-root-dir {}",
    "--prod-data-source-dir {}",
    "--start-timestamp-as-str {}",
    "--end-timestamp-as-str {}",
    f"--mode {dag_run_mode}",
    f"--run-mode '{_SYSTEM_RUN_MODE}'",
    "--no-prevent-overwriting",
    "--run-notebook",
    f"--stage {_SYSTEM_STAGE}",
    "--mark-as-last-24-hour-run",
]

start_task = DummyOperator(task_id="start_dag", dag=dag)
end_task = DummyOperator(task_id="end_system_tasks", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

for dag_builder in _DAG_BUILDERS:
    start_timestamp = "{{{{ align_to_grid(dag_run.get_task_instance('start_dag').start_date \
        + macros.timedelta(seconds=300), {}) | ts_nodash | replace('T', '_') }}}}".format(
        _DAG_BAR_DURATION_MIN[dag_builder]
    )

    end_timestamp = "{{{{ ((align_to_grid(dag_run.get_task_instance('start_dag').start_date \
        + macros.timedelta(seconds=(params.run_duration_in_sec | int) + 300), {})) \
         - macros.timedelta(minutes={})) | ts_nodash | replace('T', '_') }}}}".format(
        _DAG_BAR_DURATION_MIN[dag_builder], _DAG_BAR_DURATION_MIN[dag_builder]
    )

    log_dir_specifier = f"{start_timestamp}.{end_timestamp}"

    log_base_dir = os.path.join(
        efs_mount,
        _STAGE,
        "system_reconciliation",
        dag_builder,
        _SYSTEM_RUN_MODE,
        log_dir_specifier,
    )

    dag_builder_ctor_as_str = (
        "dataflow_{}.pipelines.{}.{}_pipeline{}.{}_DagBuilder{}".format(
            _DAG_REPO[dag_builder],
            dag_builder[:-1],
            dag_builder,
            _DAG_BUILD_CTOR_SUFFIX[dag_builder],
            dag_builder,
            _DAG_BUILD_CTOR_SUFFIX[dag_builder],
        )
    )

    curr_bash_command = copy.deepcopy(system_run_cmd)
    curr_bash_command[1] = curr_bash_command[1].format(dag_builder)
    curr_bash_command[2] = curr_bash_command[2].format(dag_builder_ctor_as_str)
    curr_bash_command[-3] = curr_bash_command[-3].format(start_timestamp)
    curr_bash_command[-2] = curr_bash_command[-2].format(
        log_base_dir, dag_run_mode
    )
    curr_bash_command[-1] = curr_bash_command[-1].format(
        log_base_dir, dag_run_mode
    )
    if _DAG_CONFIG_PARAMS.get(dag_builder):
        curr_bash_command += _DAG_CONFIG_PARAMS[dag_builder]["run"]

    curr_reconcile_command = copy.deepcopy(system_reconcile_cmd)
    curr_reconcile_command[3] = curr_reconcile_command[3].format(
        dag_builder_ctor_as_str
    )
    reconc_log_base_dir = f"{efs_mount}/{_STAGE}/prod_reconciliation/"
    source_dir = f"{efs_mount}/{_STAGE}/system_reconciliation/"
    curr_reconcile_command[4] = curr_reconcile_command[4].format(
        reconc_log_base_dir
    )
    curr_reconcile_command[5] = curr_reconcile_command[5].format(source_dir)
    curr_reconcile_command[6] = curr_reconcile_command[6].format(start_timestamp)
    curr_reconcile_command[7] = curr_reconcile_command[7].format(end_timestamp)
    if _DAG_CONFIG_PARAMS.get(dag_builder):
        curr_reconcile_command += _DAG_CONFIG_PARAMS[dag_builder]["reconcile"]

    system_run_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"{dag_builder}.system_run",
        curr_bash_command,
        _ECS_TASK_DEFINITION,
        1024,
        4096,
    )
    system_reconcile_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"{dag_builder}.system_reconcile",
        curr_reconcile_command,
        _ECS_TASK_DEFINITION,
        2048,
        12288,
        ephemeralStorageGb=40,
    )

    start_task >> system_run_task >> system_reconcile_task >> end_task

if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag, _STAGE, "trading", _DAG_ID, "{{ run_id }}"
    )
    end_task >> telegram_notification_task >> end_dag
