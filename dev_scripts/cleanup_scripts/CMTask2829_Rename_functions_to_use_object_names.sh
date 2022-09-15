#!/bin/bash -xe

dir_names="amp/dev_scripts/cleanup_scripts dev_scripts/cleanup_scripts"

replace_text.py \
  --old "build_im_client_from_config" \
  --new "build_ImClient_from_System" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "(?=\w+)_market_data_(?!\w+\(self)" \
  --new "_MarketData_" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "(?=\w+)_im_client_(?!\w+\(self)" \
  --new "_ImClient_" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "get_EventLoop_MarketData_from_df" \
  --new "get_ReplayedMarketData_from_df" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "adapt_dag_to_real_time_from_config" \
  --new "get_RealTimeDag_from_System" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "get_HistoricalDag_from_system" \
  --new "build_HistoricalDag_from_System" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "add_process_forecasts_node" \
  --new "add_ProcessForecastsNode" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "(?=\w+)_dag_runner_(?!\w+\(self)" \
  --new "_DagRunner_" \
  --exclude_dirs "$dir_names"

replace_text.py \
  --old "check_system_config" \
  --new "check_SystemConfig" \
  --exclude_dirs "$dir_names"
