#!/usr/bin/env python
"""
Run historical simulation for model configuration C14a.config1:

https://github.com/cryptokaizen/cmamp/blob/master/docs/trade_execution/ck.model_configurations.reference.md#c14aconfig1
"""
import datetime
import logging

import core.config as cconfig
import dataflow.backtest as dtfbcktst
import dataflow_amp.system.Cx.Cx_forecast_system_example as dtfasccfsex
import helpers.hdbg as hdbg

if __name__ == "__main__":
    # Set model params.
    dag_builder_ctor_as_str = (
        "dataflow_lemonade.pipelines.C14.C14a_pipeline.C14a_DagBuilder"
    )
    fit_at_beginning = False
    train_test_mode = "ins"
    # The end timestamp is set to yesterday by default to get the latest
    # full day data available.
    end_date = datetime.date.today() - datetime.timedelta(1)
    end_date_as_str = end_date.strftime("%Y-%m-%d")
    start_date_as_str = "2023-08-01"
    backtest_config = f"ccxt_v8_1-all.15T.{start_date_as_str}_{end_date_as_str}"
    # Set `ImClient` config.
    im_client_config = {
        "universe_version": "v8.1",
        "root_dir": "s3://cryptokaizen-data.preprod/v3",
        "partition_mode": "by_year_month",
        "dataset": "ohlcv",
        "contract_type": "futures",
        "data_snapshot": "",
        "aws_profile": "ck",
        "resample_1min": False,
        "version": "v1_0_0",
        # Make sure it is related to `universe_version`.
        "download_universe_version": "v8",
        "tag": "downloaded_1min",
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
    update_config_switch = True
    if update_config_switch:
        # Below there is just an example.
        config_update = cconfig.Config.from_dict(
            {
                "backtest_config": {"lookback_as_pd_str": "62T"},
            },
        )
    else:
        config_update = None
    # Set execution params.
    abort_on_error = True
    num_threads = 2
    num_attempts = 1
    dry_run = False
    backend = "threading"
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
