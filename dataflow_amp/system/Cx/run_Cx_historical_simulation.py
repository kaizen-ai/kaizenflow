"""
Import as:

import dataflow_amp.system.Cx.run_Cx_historical_simulation as dtfascrchs
"""

import logging
import os
from typing import Optional

import core.config as cconfig
import dataflow.backtest as dtfback
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import dataflow_amp.system.Cx.Cx_forecast_system_example as dtfasccfsex
import dataflow_amp.system.Cx.Cx_tile_config_builders as dtfascctcbu
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hjoblib as hjoblib
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


def _run_experiment(
    config_list: dtfsys.SystemConfigList, train_test_mode: str
) -> None:
    """
    Run experiment for the given config list and `train_test_mode`.

    :param config_list: system config list to run experiment for
    :param train_test_mode: see `run_backtest()`
    """
    if train_test_mode == "ins":
        dtfback.run_in_sample_tiled_backtest(config_list)
    elif train_test_mode == "ins_oos":
        dtfback.run_ins_oos_tiled_backtest(config_list)
    elif train_test_mode == "rolling":
        dtfback.run_rolling_tiled_backtest(config_list)
    else:
        raise ValueError(f"Unsupported `train_test_mode={train_test_mode}")


def _get_default_dst_dir(
    dag_builder_ctor_as_str: str,
    train_test_mode: str,
    backtest_config: str,
) -> str:
    """
    Get default destination dir from the system parameters.

    See param descriptions in `run_backtest()`.

    :return: dst dir, e.g., `build_tile_configs.C3a.ccxt_v7_1-all.5T.2022-06-01_2022-06-02.ins/`
    """
    # Get DagBuilder name since it's convenient to use in the dir names.
    dag_builder_name = dtfcore.get_DagBuilder_name_from_string_pointer(
        dag_builder_ctor_as_str
    )
    dst_dir = ".".join(
        [
            "./build_tile_configs",
            dag_builder_name,
            backtest_config,
            train_test_mode,
        ]
    )
    return dst_dir


def _patch_config_lists(
    config_list: cconfig.ConfigList,
    dst_dir: str,
    index: Optional[int],
    start_from_index: Optional[int],
    incremental: bool,
) -> cconfig.ConfigList:
    """
    Patch all the configs with all the necessary info to run the workload.

    See param descriptions in `run_backtest()`.
    """
    # Patch the configs with the command line parameters.
    params = {"dst_dir": dst_dir}
    config_list = cconfig.patch_config_list(config_list, params)
    # Select the configs based on command line options.
    config_list = dtfback.select_config(config_list, index, start_from_index)
    _LOG.info("Selected %d configs from command line", len(config_list))
    # Remove the configs already executed.
    config_list, num_skipped = dtfback.skip_configs_already_executed(
        config_list, incremental
    )
    _LOG.info("Removed %d configs since already executed", num_skipped)
    _LOG.info("Need to execute %d configs", len(config_list))
    return config_list


def run_backtest_for_single_config(
    config_list: dtfsys.SystemConfigList,
    train_test_mode: str,
    #
    incremental: bool,
    num_attempts: int,
) -> None:
    """
    Get `SystemConfigList` for the selected config and run the experiment.

    :param config_list: system config list to run backtest experiment for
    :param train_test_mode: see `run_backtest()`
    """
    hdbg.dassert_isinstance(config_list, dtfsys.SystemConfigList)
    _ = incremental
    hdbg.dassert_eq(1, num_attempts, "Multiple attempts not supported yet")
    #
    config = config_list.get_only_config()
    dtfback.setup_experiment_dir(config)
    # Extract params from config.
    _LOG.info("config=\n%s", config)
    idx = config[("backtest_config", "id")]
    _LOG.info("\n%s", hprint.frame(f"Executing experiment for config {idx}"))
    experiment_result_dir = config[("backtest_config", "experiment_result_dir")]
    # Run experiment.
    try:
        _ = _run_experiment(config_list, train_test_mode)
    except Exception as e:
        # Display the type of occurring error.
        _LOG.error("The error is: %s", e)
        msg = f"Execution failed for experiment {idx}"
        _LOG.error(msg)
        raise RuntimeError(msg)
    else:
        _LOG.info("Execution successful for experiment %s", idx)
        dtfback.mark_config_as_success(experiment_result_dir)


def _get_joblib_workload(
    dag_builder_ctor_as_str: str,
    fit_at_beginning: bool,
    train_test_mode: str,
    backtest_config: str,
    dst_dir: str,
    index: Optional[int],
    start_from_index: Optional[int],
    incremental: bool,
) -> hjoblib.Workload:
    """
    Prepare the joblib workload by building all the Configs.

    See param descriptions in `run_backtest()`.
    """
    # Build a system to run simulation for.
    system = dtfasccfsex.get_Cx_NonTime_ForecastSystem_for_simulation_example1(
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode=train_test_mode,
        backtest_config=backtest_config,
        # oos_start_date_as_str=oos_start_date_as_str,
    )
    # Get configs to run.
    config_list = dtfascctcbu.build_tile_config_list(system, train_test_mode)
    _LOG.info("Generated %d configs", len(config_list))
    # Patch configs with information needed to run.
    config_list = _patch_config_lists(
        config_list,
        dst_dir,
        index,
        start_from_index,
        incremental,
    )
    # Prepare one task per config to run.
    tasks = []
    for config in config_list.configs:
        # Even though the goal is to run a single config, pass a config
        # as a `SystemConfigList` object because it also carries the `System`
        # which is required by an experiment runner.
        config_list_copy = config_list.copy()
        config_list_copy.configs = [config]
        task: hjoblib.Task = (
            # args.
            (
                config_list_copy,
                train_test_mode,
            ),
            # kwargs.
            {},
        )
        tasks.append(task)
    #
    func_name = "run_backtest_for_single_config"
    workload = (run_backtest_for_single_config, func_name, tasks)
    hjoblib.validate_workload(workload)
    return workload


# TODO(Dan): Add `oos_start_date_as_str`.
def run_backtest(
    # Model params.
    dag_builder_ctor_as_str: str,
    fit_at_beginning: bool,
    train_test_mode: str,
    backtest_config: str,
    # Dir params.
    dst_dir: Optional[str],
    dst_dir_tag: Optional[str],
    clean_dst_dir: bool,
    no_confirm: bool,
    # Config params.
    index: Optional[int],
    start_from_index: Optional[int],
    # Execution params.
    # TODO(Grisha): make sure that aborting on error works.
    # Probably it does not work out of the box.
    abort_on_error: bool,
    num_threads: int,
    num_attempts: int,
    dry_run: bool,
) -> None:
    """
    Run historical simulation backtest.

    :param dag_builder_ctor_as_str: a pointer to a `DagBuilder` constructor,
        e.g., `dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder`
    :param fit_at_beginning: force the system to fit before making predictions
    :param train_test_mode: how to perform the prediction
        - "ins": fit and predict using the same data
        - "ins_oos": fit using a train dataset and predict using a test dataset
        - "rolling": see `RollingFitPredictDagRunner` for description
    :param backtest_config: backtest_config, e.g.,`ccxt_v7-all.5T.2022-09-01_2022-11-30`
    :param dst_dir: experiment results destination directory,
        e.g., `build_tile_configs.C3a.ccxt_v7_1-all.5T.2022-06-01_2022-06-02.ins/`
    :param dst_dir_tag: a tag to add to the experiment results directory
    :param clean_dst_dir: opposite of `incremental` form `hio.create_dir()`
    :param no_confirm: opposite of `ask_to_delete` form `hio.create_dir()`
    :param index: index of a config to execute, if not `None`
    :param start_from_index: index of a config to start execution from,
        if not `None`
    :param abort_on_error: when True, if one task asserts then stop executing
        the workload and return the exception of the failing task
        - If False, the execution continues
    :param num_threads: joblib parameter to control how many threads to use
    :param dry_run: if True, print the workload and exit without executing it
    """
    # TODO(Grisha): make it work with multiple parallel threads.
    hdbg.dassert_eq(
        "serial",
        num_threads,
        "Multiple execition threads are not supported yet.",
    )
    # Set destination dir.
    if not dst_dir:
        # Use the default destination dir, see the function below.
        dst_dir = _get_default_dst_dir(
            dag_builder_ctor_as_str,
            train_test_mode,
            backtest_config,
        )
    if dst_dir_tag:
        dst_dir = ".".join([dst_dir, dst_dir_tag])
    # TODO(Grisha): We could use `incremental` and `ask_to_delete` directly,
    # keeping `clean_dst_dir` and `no_confirm` so that the interface look like
    # the one from `run_config_list.py`.
    incremental = not clean_dst_dir
    ask_to_delete = not no_confirm
    hio.create_dir(dst_dir, incremental, ask_to_delete=ask_to_delete)
    # Prepare the workload.
    workload = _get_joblib_workload(
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode,
        backtest_config,
        dst_dir,
        index,
        start_from_index,
        incremental,
    )
    # Prepare the log file.
    timestamp = hdateti.get_current_timestamp_as_string("naive_ET")
    # TODO(Grisha): have a detailed log with info about each experiment,
    # now the log contains only the high-level info, i.e. a config and if
    # an experiment failed or not.
    log_file = os.path.join(dst_dir, f"log.{timestamp}.txt")
    _LOG.info("log_file='%s'", log_file)
    # Execute.
    # backend = "loky"
    # TODO(gp): Is this the correct backend? It might not matter since we spawn
    # a process with system.
    backend = "asyncio_threading"
    hjoblib.parallel_execute(
        workload,
        dry_run,
        num_threads,
        incremental,
        abort_on_error,
        num_attempts,
        log_file,
        backend=backend,
    )
    _LOG.info("dst_dir='%s'", dst_dir)
    _LOG.info("log_file='%s'", log_file)
    # TODO(Dan): Add archiving on S3.
