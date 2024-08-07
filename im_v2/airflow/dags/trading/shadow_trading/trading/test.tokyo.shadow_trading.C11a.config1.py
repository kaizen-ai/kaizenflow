"""
Pre-production shadow trading DAG that is running when performing pre-release
runs.

Note that this DAG should not be changed for any test purposes but pre-
release runs.
"""

import copy
import datetime
import os

import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
import airflow_utils.trade_exec.utils as aitrexut
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
_STAGE = aiutmisc.get_stage_from_filename(_FILENAME)

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = "tokyo-full-system-preprod"

_DAG_ID = aiutmisc.get_dag_id_from_filename(_FILENAME)
_DAG_BUILDER = "C11a"
_TAG = "config1"
_DAG_BUILDER_CTOR = (
    "dataflow_lemonade.pipelines.C11.C11a_pipeline.C11a_DagBuilder"
)
_DAG_CONFIG_PARAMS = {
    "C11a": {
        "config1": {
            "run": [
                '--set_config_value \'("dag_property_config","debug_mode_config","save_node_io"),(str("df_as_pq"))\'',
                '--set_config_value \'("dag_property_config","debug_mode_config","save_node_df_out_stats"),(bool(True))\'',
                '--set_config_value \'("dag_property_config","force_free_nodes"),(bool(False))\'',
                '--set_config_value \'("dag_property_config","debug_mode_config","profile_execution"),(bool(True))\'',
                '--set_config_value \'("dag_config","resample","transformer_kwargs","rule"),(str("5T"))\'',
                '--set_config_value \'("market_data_config","days"),(pd.Timedelta(str("6T")))\'',
                '--set_config_value \'("market_data_config","universe_version"),(str("v8.1"))\'',
                '--set_config_value \'("trading_period"),(str("5T"))\'',
                '--set_config_value \'("portfolio_config", "pricing_method"),(str("last"))\'',
                '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(5))\'',
                '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","backend"),(str("batch_optimizer"))\'',
                '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params"),({"dollar_neutrality_penalty": float(0.0), "constant_correlation": float(0.5), "constant_correlation_penalty": float(50.0), "relative_holding_penalty": float(0.0), "relative_holding_max_frac_of_gmv": float(0.1), "target_gmv": float(100000.0), "target_gmv_upper_bound_penalty": float(0.0), "target_gmv_hard_upper_bound_multiple": float(1.05), "transaction_cost_penalty": float(0.5), "solver": str("ECOS")})\'',
                '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","order_config","order_type"),(str("price@start"))\'',
                '--set_config_value \'("process_forecasts_node_dict","process_forecasts_dict","liquidate_at_trading_end_time"),(bool(False))\'',
            ],
            "reconcile": [
                '--set-config-values \'("process_forecasts_node_dict","process_forecasts_dict","liquidate_at_trading_end_time"),(bool(False));("market_data_config","days"),(pd.Timedelta(str("6T")));("trading_period"),(str("5T"));("portfolio_config", "pricing_method"),(str("last"));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_duration_in_mins"),(int(15));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","backend"),(str("batch_optimizer"));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params"),({"dollar_neutrality_penalty": float(0.0), "constant_correlation": float(0.5), "constant_correlation_penalty": float(0.0), "relative_holding_penalty": float(0.0), "relative_holding_max_frac_of_gmv": float(0.1), "target_gmv": float(100000.0), "target_gmv_upper_bound_penalty": float(0.0), "target_gmv_hard_upper_bound_multiple": float(1.05), "transaction_cost_penalty": float(0.35), "solver": str("ECOS")});("market_data_config","universe_version"),(str("v8.1"));("process_forecasts_node_dict","process_forecasts_dict","order_config","order_type"),(str("price@start"))\'',
            ],
        },
    }
}

_DAG_BAR_DURATION_MIN = 5
_LIVENESS = "CANDIDATE"
_INSTANCE_TYPE = "PROD"
_VERBOSITY = "DEBUG"
_EXCHANGE = "binance"
_SYSTEM_STAGE = "preprod"
_ACCOUNT_TYPE = "trading"
_SECRET_ID = 3
_DAG_DESCRIPTION = "Pre-production shadow trading DAG."
# Run duration (specified in hours so it can be propagated to both container
# ans Airflow 'cron' schedule).
_SYSTEM_RUN_MODE = "paper_trading"
_RUN_DURATION = 24
_RUN_DURATION_IN_SECS = int(_RUN_DURATION) * 3600
_SCHEDULE = "30 11 * * * "
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, True, _USERNAME)
_S3_BUCKET_PATH_FOR_EXPERIMENT = aiutmisc.create_s3_bucket_path_for_experiment(
    Variable.get(f"{_STAGE}_s3_data_bucket"), "tokyo_experiments_compressed"
)
# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": 0,
    "email": "",
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
    },
    start_date=datetime.datetime(2022, 8, 1, 0, 0, 0),
    tags=[_STAGE],
)


# #############################################################################
# Declare system cmd variables.
# #############################################################################

efs_mount = "{{ var.value.efs_mount_tokyo }}"
# We use data_interval_end because the DAG is supposed to start in real-time.
dag_run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
run_date = "{{ dag_run.get_task_instance('start_dag').start_date.date() | string | replace('-', '') }}"
run_mode_for_log = "{{ '' if 'scheduled' in run_id else '.manual' }}"

start_timestamp = "{{{{ align_to_grid(dag_run.get_task_instance('start_dag').start_date \
    + macros.timedelta(seconds=300), {}) | ts_nodash | replace('T', '_') }}}}".format(
    _DAG_BAR_DURATION_MIN
)

end_timestamp = "{{{{ ((align_to_grid(dag_run.get_task_instance('start_dag').start_date \
    + macros.timedelta(seconds=(params.run_duration_in_sec | int) + 300), {})) \
    - macros.timedelta(minutes={})) | ts_nodash | replace('T', '_') }}}}".format(
    _DAG_BAR_DURATION_MIN, _DAG_BAR_DURATION_MIN
)

log_dir_specifier = f"{start_timestamp}.{end_timestamp}"
log_base_dir = os.path.join(
    efs_mount,
    _STAGE,
    "system_reconciliation",
    f"{_DAG_BUILDER}.{_TAG}",
    _SYSTEM_RUN_MODE,
    log_dir_specifier,
)

# #############################################################################
# Build system run command.
# #############################################################################

system_run_cmd = [
    "/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py",
    f"--strategy '{_DAG_BUILDER}'",
    f"--dag_builder_ctor_as_str '{_DAG_BUILDER_CTOR}'",
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
    f"--start_time '{start_timestamp}'",
    f"--log_file_name  '{log_base_dir}/logs/log.{dag_run_mode}.txt'",
    f"--dst_dir '{log_base_dir}/system_log_dir.{dag_run_mode}'",
]

if _DAG_CONFIG_PARAMS.get(_DAG_BUILDER):
    system_run_cmd += _DAG_CONFIG_PARAMS[_DAG_BUILDER][_TAG]["run"]

# #############################################################################
# Build system reconciliation command.
# #############################################################################

log_dir_specifier = f"{start_timestamp}.{end_timestamp}"
reconc_log_base_dir = f"{efs_mount}/{_STAGE}/prod_reconciliation/"
source_dir = f"{efs_mount}/{_STAGE}/system_reconciliation/"
#
system_reconcile_cmd = [
    # This is a hack to pass hserver.is_inside_docker()
    #  because when ran in ECS it is not present there
    #  by default.
    "mkdir /.dockerenv",
    "&&",
    "invoke reconcile_run_all",
    f"--dag-builder-ctor-as-str '{_DAG_BUILDER_CTOR}'",
    f"--dst-root-dir {reconc_log_base_dir}",
    f"--prod-data-source-dir {source_dir}",
    f"--start-timestamp-as-str {start_timestamp}",
    f"--end-timestamp-as-str {end_timestamp}",
    f"--mode {dag_run_mode}",
    f"--run-mode '{_SYSTEM_RUN_MODE}'",
    "--no-prevent-overwriting",
    "--run-notebook",
    f"--stage {_SYSTEM_STAGE}",
    f"--tag {_TAG}",
    "--mark-as-last-24-hour-run",
]
#
if _DAG_CONFIG_PARAMS.get(_DAG_BUILDER):
    system_reconcile_cmd += _DAG_CONFIG_PARAMS[_DAG_BUILDER][_TAG]["reconcile"]

# #############################################################################
# Build command to move experiment directory to EU EFS.
# #############################################################################

# Compress experiment directory and upload to S3.
compress_experiment_dir_cmd = aitrexut.get_compress_experiment_dir_cmd(_STAGE)
# Fetch experiment from S3, extract into Europe EFS.
move_experiment_to_eu_efs_cmd = aitrexut.get_move_experiment_to_eu_efs_cmd(_STAGE)
# TODO(Nina): Update to use f-strings instead of `format`.
log_compress_base_dir = os.path.join(
    "system_reconciliation",
    f"{_DAG_BUILDER}.{_TAG}",
    _SYSTEM_RUN_MODE,
    log_dir_specifier,
)
curr_compress_experiment_dir_cmd = copy.deepcopy(compress_experiment_dir_cmd)
curr_compress_experiment_dir_cmd[2] = curr_compress_experiment_dir_cmd[2].format(
    f"{_DAG_BUILDER}.{log_dir_specifier}.run", log_compress_base_dir
)
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


# #############################################################################
# Run tasks.
# #############################################################################


start_task = DummyOperator(task_id="start_dag", dag=dag)
end_task = DummyOperator(task_id="end_system_tasks", dag=dag)
end_dag = DummyOperator(task_id="end_dag", dag=dag)

system_run_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"{_DAG_BUILDER}.{_TAG}.system_run",
    system_run_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=2048,
    memory_mb=10240,
    region=aiutecop.ASIA_REGION,
)
system_reconcile_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"{_DAG_BUILDER}.{_TAG}.system_reconcile",
    system_reconcile_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=2048,
    memory_mb=12288,
    ephemeralStorageGb=40,
    region=aiutecop.ASIA_REGION,
)

# Create task to compress experiment.
compress_experiment_dir_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    "compress_experiment_dir",
    curr_compress_experiment_dir_cmd,
    _ECS_TASK_DEFINITION,
    cpu_units=512,
    memory_mb=1024,
    region=aiutecop.ASIA_REGION,
)
compress_experiment_dir_task.trigger_rule = "all_done"
# Create task to move experiment to EU EFS.
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
    >> system_run_task
    >> system_reconcile_task
    >> compress_experiment_dir_task
    >> move_experiment_dir_to_eu_efs_task
    >> end_task
)
