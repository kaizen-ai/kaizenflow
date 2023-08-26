#!/usr/bin/env python
import logging

import dataflow_amp.system.Cx.run_Cx_historical_simulation as dtfascrchs
import helpers.hdbg as hdbg


if __name__ == "__main__":
    # Set model params.
    dag_builder_ctor_as_str = "dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
    fit_at_beginning = False
    train_test_mode = "ins"
    backtest_config = "ccxt_v7-all.5T.2022-09-01_2022-11-30"
    # Set dir params.
    dst_dir = None
    dst_dir_tag = "run0"
    clean_dst_dir = True
    no_confirm = True
    # Set config params.
    index = None
    start_from_index = None
    # Set execution params.
    abort_on_error = True
    num_threads = "serial"
    num_attempts = 1
    dry_run = False
    log_level = logging.DEBUG
    # Set logger.
    hdbg.init_logger(
        verbosity=log_level,
        use_exec_path=True,
        # report_memory_usage=True,
    )
    # Run.
    dtfascrchs.run_backtest(
        # Model params.
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode,
        backtest_config,
        # Dir params.
        dst_dir,
        dst_dir_tag,
        clean_dst_dir,
        no_confirm,
        # Config params.
        index,
        start_from_index,
        # Execution params.
        abort_on_error,
        num_threads,
        num_attempts,
        dry_run,
    )
