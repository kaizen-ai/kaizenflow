#!/usr/bin/env python

"""
This is a copy of `Cx_template.run_historical_simulation.py` adapted for the
bid/ask mock pipeline.
"""

import logging

import core.config as cconfig
import dataflow.backtest as dtfbcktst
import dataflow_amp.system.Cx.Cx_forecast_system_example as dtfasccfsex
import helpers.hdbg as hdbg

if __name__ == "__main__":
    # Set model params.
    dag_builder_ctor_as_str = "dataflow_amp.pipelines.mock_bid_ask.mock_bid_ask_pipeline.MockBidAsk_DagBuilder"
    fit_at_beginning = False
    train_test_mode = "ins"
    backtest_config = "ccxt_v7_4-all.5T.2024-01-01_2024-02-29"
    # Set `ImClient` config.
    im_client_config = {
        "universe_version": "v7.4",
        "root_dir": "s3://cryptokaizen-data.preprod/v3",
        "partition_mode": "by_year_month",
        "dataset": "bid_ask",
        "contract_type": "futures",
        "data_snapshot": "",
        "aws_profile": "ck",
        "resample_1min": False,
        "version": "v2_0_0",
        "download_universe_version": "v7",
        "tag": "resampled_1min",
    }
    # Set destination dir params.
    dst_dir = None
    dst_dir_tag = "run0"
    clean_dst_dir = True
    no_confirm = True
    # Set config params.
    index = None
    start_from_index = None
    # Introduce a switch instead of commenting out, otherwise the Linter
    # removes the `cconfig` import.
    update_config_switch = False
    if update_config_switch:
        # Below there is just an example.
        config_update = cconfig.Config.from_dict(
            {
                "dag_builder_class": "C1b_DagBuilder",
                "dag_config": {
                    "resample": {
                        "transformer_kwargs": {
                            "rule": "1T",
                        },
                    },
                },
            },
        )
    else:
        config_update = None
    # Set execution params.
    abort_on_error = True
    num_threads = 2
    num_attempts = 1
    dry_run = False
    backend = "multiprocessing"
    # Set logger.
    log_level = logging.DEBUG
    hdbg.init_logger(
        verbosity=log_level,
        use_exec_path=True,
        # report_memory_usage=True,
    )
    # Create system.
    system = dtfasccfsex.get_Cx_NonTime_ForecastSystem_example(
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode=train_test_mode,
        backtest_config=backtest_config,
        im_client_config=im_client_config,
    )
    # Run.
    dtfbcktst.run_backtest(
        # Model params.
        system,
        config_update,
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
        backend,
    )
