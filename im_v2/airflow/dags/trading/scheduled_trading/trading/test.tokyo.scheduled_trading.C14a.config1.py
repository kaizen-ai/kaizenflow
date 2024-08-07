"""
Pre-production scheduled trading system DAG that is running when performing
pre-release runs.

Note that this DAG should not be changed for any test purposes but pre-
release runs.
"""

import copy
import datetime
import os

import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
import airflow_utils.telegram.operator as aiutteop
import airflow_utils.trade_exec.utils as aitrexut
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
dag_type = "trading"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]
_DAG_ID = components_dict["dag_id"]
_REGION = (
    aiutecop.ASIA_REGION
    if components_dict["location"] == "tokyo"
    else aiutecop._EUROPE_REGION
)
# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = "tokyo-full-system-preprod"
#
_DAG_BUILDER = components_dict["model"]
_TAG = "config1"
_DAG_BUILDER_CTOR = (
    "dataflow_lemonade.pipelines.C14.C14a_pipeline.C14a_DagBuilder"
)
_DAG_CONFIG_PARAMS = {
    "run": [
        '--set_config_value \'("dag_property_config","debug_mode_config","save_node_io"),(str(""))\'',
        '--set_config_value \'("dag_property_config","debug_mode_config","save_node_df_out_stats"),(bool(False))\'',
        '--set_config_value \'("dag_property_config","force_free_nodes"),(bool(False))\'',
        '--set_config_value \'("dag_property_config","debug_mode_config","profile_execution"),(bool(False))\'',
        '--set_config_value \'("market_data_config","days"),(pd.Timedelta(str("510T")))\'',
        '--set_config_value \'("market_data_config","universe_version"),(str("v8.1"))\'',
        '--set_config_value \'("trading_period"),(str("15T"))\'',
        '--set_config_value \'("portfolio_config", "pricing_method"),(str("last"))\'',
        '--set_config_value \'("portfolio_config","broker_config"),({"limit_price_computer_type": "LimitPriceComputerUsingVolatility", "limit_price_computer_kwargs": {"volatility_multiple": '
        + "{{ params.volatility_multiple }} }})'",
        '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","liquidate_at_trading_end_time"),(bool(True))\'',
        '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(15))\'',
        '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","backend"),(str("batch_optimizer"))\'',
        '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","order_config","execution_frequency"),(str("10S"))\'',
        '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params"),({"dollar_neutrality_penalty": float(0.0), "constant_correlation": float(0.5), "constant_correlation_penalty": float(0.0), "relative_holding_penalty": float(0.0), "relative_holding_max_frac_of_gmv": float(0.1), "target_gmv": float(1000.0), "target_gmv_upper_bound_penalty": float(0.0), "target_gmv_hard_upper_bound_multiple": float(1.05), "transaction_cost_penalty": float(0.35), "solver": str("ECOS")})\'',
    ],
    "reconcile": [
        '--set-config-values \'("dag_property_config","force_free_nodes"),(bool(False));("dag_property_config","debug_mode_config","profile_execution"),(bool(True));("market_data_config","days"),(pd.Timedelta(str("510T")));("trading_period"),(str("15T"));("portfolio_config", "pricing_method"),(str("last"));("process_forecasts_node_dict","process_forecasts_dict","liquidate_at_trading_end_time"),(bool(True));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(15));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","backend"),(str("batch_optimizer"));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params"),({"dollar_neutrality_penalty": float(0.0), "constant_correlation": float(0.5), "constant_correlation_penalty": float(0.0), "relative_holding_penalty": float(0.0), "relative_holding_max_frac_of_gmv": float(0.1), "target_gmv": float(1000.0), "target_gmv_upper_bound_penalty": float(0.0), "target_gmv_hard_upper_bound_multiple": float(1.05), "transaction_cost_penalty": float(0.35), "solver": str("ECOS")});("market_data_config","universe_version"),(str("v8.1"));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_type"),(str("price@start"))\''
    ],
}
_DAG_BAR_DURATION_MIN = {"C14a": 15}
_UNIVERSE = "v8.1"
_LIVENESS = "CANDIDATE"
_INSTANCE_TYPE = "PROD"
_VERBOSITY = "INFO"
_EXCHANGE = "binance"
# Corresponds to the DB stage.
_SYSTEM_STAGE = "preprod"
_ACCOUNT_TYPE = "trading"
_SECRET_ID = 4
_DAG_DESCRIPTION = "Preprod Live System DAG"
# Run duration (specified in hours so it can be propagated to both container
# ans Airflow 'cron' schedule).
# Choose "prod" to run with live money, "paper_trading" otherwise.
_SYSTEM_RUN_MODE = "prod"
_RUN_DURATION_IN_SECS = 7200
_SCHEDULE = None
_VOLATILITY_MULTIPLE = "[float(1.5), float(0.7), float(0.7), float(0.7), float(0.7), float(0.7), float(0.7), float(0.7), float(0.7), float(0.6), float(0.6), float(0.5), float(0.25), float(0.25), float(0.25)] + ([10.0]*78)"
_S3_BUCKET_PATH_FOR_EXPERIMENT = aiutmisc.create_s3_bucket_path_for_experiment(
    Variable.get(f'{_STAGE}_s3_data_bucket'),
    "tokyo_experiments_compressed"
)
efs_mount = (
    aiutecop._EFS_MOUNT_TOKYO
    if components_dict["location"] == "tokyo"
    else aiutecop._EFS_MOUNT_EU
)
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": 0,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": True,
    "owner": "airflow",
}


# TODO(Juraj): add docstrings.
def get_bar_duration(dag_builder: str) -> int:
    return _DAG_BAR_DURATION_MIN[dag_builder]


# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=2,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    user_defined_filters={
        "get_bar_duration": get_bar_duration,
    },
    user_defined_macros={
        "align_to_grid": aiutmisc.align_to_grid,
    },
    params={
        "run_duration_in_sec": Param(
            _RUN_DURATION_IN_SECS, "How long should the run be (in seconds)"
        ),
        "secret_id": Param(
            _SECRET_ID,
            "Specify the secret ID for the trading account. For a current list of account IDs and their mappings, refer to the Trading Account List in docs/trading_ops/ck.trading.how_to_guide.md#trading-account-list",
        ),
        "target_dollar_risk_per_name": Param(0.2),
        "prediction_abs_threshold": Param(0.3),
        "model": Param(_DAG_BUILDER, enum=["C14a"]),
        "verbosity": Param(_VERBOSITY, enum=["INFO", "DEBUG"]),
        "volatility_multiple": Param(_VOLATILITY_MULTIPLE),
    },
    start_date=datetime.datetime(2022, 8, 1, 0, 0, 0),
    tags=[_STAGE],
)

efs_mount = "{{ var.value.efs_mount_tokyo }}"
# We use data_interval_end because the DAG is supposed to start in real-time.
dag_run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
run_date = "{{ dag_run.get_task_instance('start_dag').start_date.date() | string | replace('-', '') }}"
run_duration = "{{ params.run_duration_in_sec }}"

system_reconcile_cmd = [
    # This is a hack to pass hserver.is_inside_docker()
    #  because when ran in ECS it is not present there
    #  by default.
    "mkdir /.dockerenv",
    "&&",
    "invoke reconcile_run_all",
    f"--dag-builder-ctor-as-str '{_DAG_BUILDER_CTOR}'",
    "--dst-root-dir {}",
    "--prod-data-source-dir {}",
    "--start-timestamp-as-str {}",
    "--end-timestamp-as-str {}",
    f"--mode {dag_run_mode}",
    f"--run-mode '{_SYSTEM_RUN_MODE}'",
    "--no-prevent-overwriting",
    "--run-notebook",
    f"--stage {_SYSTEM_STAGE}",
    f"--tag {_TAG}",
    "--backup-dir-if-exists",
]

log_full_bid_ask_data = aitrexut.get_log_full_bid_ask_data_cmd(_UNIVERSE, "Binance", _EXCHANGE)

run_notebooks_cmd = aitrexut.get_run_notebook_cmd(
    "{}/system_log_dir.{}/process_forecasts"
)
# Compress experiment directory and upload to S3.
compress_experiment_dir_cmd = aitrexut.get_compress_experiment_dir_cmd(_STAGE)

# Fetch experiment from S3, extract into Europe EFS.
move_experiment_to_eu_efs_cmd = aitrexut.get_move_experiment_to_eu_efs_cmd(_STAGE)

# Create tasks.
start_task = DummyOperator(task_id="start_dag", dag=dag)
end_task = DummyOperator(task_id="end_system_tasks", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

# Calculate start and end timestamps.
start_timestamp = "{{ align_to_grid(dag_run.get_task_instance('start_dag').start_date \
    + macros.timedelta(seconds=480), params.model | get_bar_duration) | ts_nodash | replace('T', '_') }}"

end_timestamp = "{{ ((align_to_grid(dag_run.get_task_instance('start_dag').start_date \
    + macros.timedelta(seconds=(params.run_duration_in_sec | int) + 480), params.model | get_bar_duration)) \
        - macros.timedelta(minutes=params.model | get_bar_duration)) | ts_nodash | replace('T', '_') }}"

# Calculate end timestamps for bid ask.
end_timestamp_for_bid_ask = "{{ (align_to_grid(dag_run.get_task_instance('start_dag').start_date \
    + macros.timedelta(seconds=(params.run_duration_in_sec | int) + 480), params.model | get_bar_duration)) \
     | ts_nodash | replace('T', '_') }}"

# Build path to timestamp directory.
log_dir_specifier = f"{start_timestamp}.{end_timestamp}"
dag_builder_dir = f"{_DAG_BUILDER}.{_TAG}"
log_base_dir = os.path.join(
    efs_mount,
    _STAGE,
    "system_reconciliation",
    dag_builder_dir,
    _SYSTEM_RUN_MODE,
    log_dir_specifier,
)

# Build system reconciliation command.
curr_reconcile_command = copy.deepcopy(system_reconcile_cmd)
reconc_log_base_dir = f"{efs_mount}/{_STAGE}/prod_reconciliation/"
source_dir = f"{efs_mount}/{_STAGE}/system_reconciliation/"
curr_reconcile_command[4] = curr_reconcile_command[4].format(reconc_log_base_dir)
curr_reconcile_command[5] = curr_reconcile_command[5].format(source_dir)
curr_reconcile_command[6] = curr_reconcile_command[6].format(start_timestamp)
curr_reconcile_command[7] = curr_reconcile_command[7].format(end_timestamp)
curr_reconcile_command += _DAG_CONFIG_PARAMS["reconcile"]

# Build logging bid/ask data command.
log_full_bid_ask_data[2] = log_full_bid_ask_data[2].format(start_timestamp)
log_full_bid_ask_data[3] = log_full_bid_ask_data[3].format(
    end_timestamp_for_bid_ask
)
log_full_bid_ask_data[4] = log_full_bid_ask_data[4].format(
    log_base_dir, dag_run_mode
)

# Build `run_notebooks` command.
curr_run_notebooks_command = copy.deepcopy(run_notebooks_cmd)
curr_run_notebooks_command[1] = curr_run_notebooks_command[1].format(
    log_base_dir, dag_run_mode
)

# Build copying data to Europe commands.
log_compress_base_dir = os.path.join(
    "system_reconciliation",
    dag_builder_dir,
    _SYSTEM_RUN_MODE,
    log_dir_specifier,
)
curr_compress_experiment_dir_cmd = copy.deepcopy(compress_experiment_dir_cmd)
curr_compress_experiment_dir_cmd[2] = curr_compress_experiment_dir_cmd[2].format(
    f"{_DAG_BUILDER}.{log_dir_specifier}.run", log_compress_base_dir
)
#
curr_compress_experiment_dir_cmd[4] = curr_compress_experiment_dir_cmd[4].format(
    f"{_DAG_BUILDER}.{log_dir_specifier}.reconciliation",
    log_compress_base_dir.replace("system_reconciliation", "prod_reconciliation"),
)
curr_move_experiment_to_eu_efs_cmd = copy.deepcopy(move_experiment_to_eu_efs_cmd)
curr_move_experiment_to_eu_efs_cmd[0] = curr_move_experiment_to_eu_efs_cmd[
    0
].format(
    _S3_BUCKET_PATH_FOR_EXPERIMENT, f"{_DAG_BUILDER}.{log_dir_specifier}*.tar.gz"
)

# Set system run config override params and values.
set_config_value_params = _DAG_CONFIG_PARAMS["run"]
# Build system run command.
system_run_cmd = aitrexut.build_system_run_cmd(
    _DAG_BUILDER,
    _DAG_BUILDER_CTOR,
    run_date,
    _LIVENESS,
    _INSTANCE_TYPE,
    _EXCHANGE,
    _SYSTEM_STAGE,
    _ACCOUNT_TYPE,
    _SECRET_ID,
    _VERBOSITY,
    run_duration,
    _SYSTEM_RUN_MODE,
    start_timestamp,
    log_base_dir,
    dag_run_mode,
    set_config_value_params,
)

# Flatten account before run.
# E.g., /data/shared/ecs/system_reconciliation/C11a/prod/20240404_114000.20240405_113000/flatten_account_before
flatten_log_dir = os.path.join(log_base_dir, "flatten_account.before")
flatten_account_before_cmd = aitrexut.get_flatten_account_cmd(
    _EXCHANGE, "{{ params.secret_id }}", _UNIVERSE, flatten_log_dir
)
flatten_account_task_before = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"flatten_account_before",
    flatten_account_before_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=1024,
    memory_mb=2048,
    region=_REGION,
)
# Run the system.
system_run_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"system_run",
    system_run_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=4096,
    memory_mb=30720,
    region=_REGION,
)
# Run the system reconciliation.
system_reconcile_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"system_reconcile",
    curr_reconcile_command,
    _ECS_TASK_DEFINITION,
    cpu_units=2048,
    memory_mb=8192,
    region=_REGION,
)
system_reconcile_task.trigger_rule = "all_done"
# Flatten account after run.
# E.g., /data/shared/ecs/system_reconciliation/C11a/prod/20240404_114000.20240405_113000/flatten_account_after
flatten_log_dir = os.path.join(log_base_dir, "flatten_account.after")
flatten_account_after_cmd = aitrexut.get_flatten_account_cmd(
    _EXCHANGE, "{{ params.secret_id }}", _UNIVERSE, flatten_log_dir
)
flatten_account_task_after = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"flatten_account_after",
    flatten_account_after_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=2048,
    memory_mb=8192,
    region=_REGION,
)
flatten_account_task_after.trigger_rule = "all_done"
# Log bid/ask data.
log_full_bid_ask_data_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"log_full_bid_ask_data",
    log_full_bid_ask_data,
    _ECS_TASK_DEFINITION,
    cpu_units=4096,
    memory_mb=30720,
    region=_REGION,
)
log_full_bid_ask_data_task.trigger_rule = "all_done"
# Run notebooks.
run_notebooks = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"run_notebooks",
    curr_run_notebooks_command + ["--run-mode 'skip_reconciliation'"],
    _ECS_TASK_DEFINITION,
    cpu_units=4096,
    memory_mb=30720,
    region=_REGION,
)
# Publish reconciliation notebook.
publish_reconciliation_notebook = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"publish_reconciliation_notebook",
    curr_run_notebooks_command + ["--run-mode 'reconciliation_only'"],
    _ECS_TASK_DEFINITION,
    cpu_units=256,
    memory_mb=512,
    region=_REGION,
)
# Publish trading report notebook.
publish_trading_report_notebook = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"publish_trading_report_notebook",
    curr_run_notebooks_command + ["--run-mode 'trading_report_only'"],
    _ECS_TASK_DEFINITION,
    cpu_units=256,
    memory_mb=2048,
    region=_REGION,
)
publish_trading_report_notebook.trigger_rule = "all_done"
# Copy data to Europe.
compress_experiment_dir_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    "compress_experiment_dir",
    curr_compress_experiment_dir_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=512,
    memory_mb=1024,
    region=_REGION,
)
compress_experiment_dir_task.trigger_rule = "all_done"
move_experiment_dir_to_eu_efs_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    "move_experiment_dir_to_eu_efs",
    # This runs a cmdline so the task definition is irrelevant.
    # But it needs to happen in Europe.
    curr_move_experiment_to_eu_efs_cmd,
    "cmamp-preprod",
    cpu_units=512,
    memory_mb=1024,
)
move_experiment_dir_to_eu_efs_task.trigger_rule = "all_done"
(
    start_task
    >> flatten_account_task_before
    >> system_run_task
    >> flatten_account_task_after
    >> log_full_bid_ask_data_task
    >> run_notebooks
    >> publish_trading_report_notebook
    >> compress_experiment_dir_task
    >> move_experiment_dir_to_eu_efs_task
    >> end_task
    >> end_dag
)

(
    flatten_account_task_after
    >> system_reconcile_task
    >> publish_reconciliation_notebook
    >> publish_trading_report_notebook
)
# Send notification on a failure.
telegram_notification_task = aiutteop.get_telegram_operator(
    dag, _STAGE, "trading", _DAG_ID, "{{ run_id }}"
)
#
end_task >> telegram_notification_task >> end_dag
publish_reconciliation_notebook >> telegram_notification_task
system_run_task >> telegram_notification_task
