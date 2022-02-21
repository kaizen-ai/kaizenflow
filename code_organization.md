<!--ts-->
   * [Code organization of amp](#code-organization-of-amp)
      * [Conventions](#conventions)
      * [Component dirs](#component-dirs)
      * [Top level dirs](#top-level-dirs)
         * [helpers](#helpers)
         * [core](#core)
         * [dataflow](#dataflow)
         * [im](#im)
         * [market_data](#market_data)
         * [oms](#oms)
         * [research_amp](#research_amp)
   * [All Python files](#all-python-files)
   * [Invariants](#invariants)
   * [Misc](#misc)



<!--te-->

# Code organization of `amp`

## Conventions

- In this file we use the following conventions:
  - Comments: `"""foobar is ..."""`
  - Dirs and subdirs: `/foobar`
  - Files: `foobar.py`
  - Objects: `FooBar`
  - Markdown files: `foobar.md`

## Component dirs

- The directories, subdirectory, objects are listed in order of their
  dependencies (from innermost to outermost)

- `/helpers`
  - """Low-level helpers that are general and not specific of this project"""

- `/core`
  - """Low-level helpers that are specific of this project"""
  - `/config`
    - `Config`
      - """An dict-like object that allows to configure workflows"""
  - `/event_study`
  - `artificial_signal_generators.py`
  - `features.py`
  - `finance.py`
  - `signal_processing.py`
  - `statitstics.py`

- `/devops`
- `/dev_scripts`
- `/documentation`

- `/im`
- `/im_v2`
  - """Instrument Master"""
  - `ImClient`
  - """Vendor specific `ImClient`s"""
- `/market_data`
  - """Interface to read price data"""
  - `MarketData`
  - `ImClientMarketData`
  - `RealTimeMarketData`
  - `ReplayedMarketData`

- `/dataflow`
  - """DataFlow module"""
  - `/core`
    - `/nodes`
      - """Implementation of DataFlow nodes that don't depend on anything
        outside of this directory"""
      - `base.py`
        - `FitPredictNode`
        - `DataSource`
      - `sources`
        - `FunctionDataSource`
        - `DfDataSource`
        - `ArmaDataSource`
      - `sinks.py`
        - `WriteCols`
        - `WriteDf`
      - `transformers.py`
      - `volatility_models.py`
      - `sklearn_models.py`
      - `unsupervided_sklearn_models.py`
      - `supervided_sklearn_models.py`
      - `regression_models.py`
      - `sarimax_models.py`
      - `gluonts_models.py`
      - `local_level_models.py`
      - `Dag`
      - `DagBuilders`
      - `DagAdapters`
      - `DagRunners`
      - `ResultBundle`
  - `/pipelines`
    - """DataFlow pipelines that use only `core` nodes"""
    - `/event_study`
    - `/features`
      - """General feature pipelines"""
    - `/price`
      - """Pipelines computing prices"""
    - `/real_times`
      - TODO(gp): -> dataflow/system
    - `/returns`
      - """Pipelines computing returns"""
    - `dataflow_example.py`
      - `NaivePipeline`
  - `/system`
    - """DataFlow pipelines with anything that depends on code outside of
      DataFlow"""
    - `source_nodes.py`
      - `DataSource`
      - `HistoricalDataSource`
      - `RealTimeDataSource`
    - `sink_nodes.py`
      - `ProcessForecasts`
    - `RealTimeDagRunner`
    - `RealTimeDagAdapter`
    - `ResearchDagAdapter`
  - `/model`
    - """Code for evaluating a DataFlow model"""
- `/oms`
  - """Order management system"""
  - `architecture.md`
  - `Broker`
  - `Order`
  - `OrderProcessor`
  - `Portfolio`
  - `ForecastProcessor`
- `/optimizer`
- `/research_amp`

## Top level dirs

```text
(cd amp; tree -L 1 -d --charset=ascii -I "*test*|*notebooks*" 2>&1 | tee /tmp/tmp)
.
|-- core
|-- dataflow
|-- helpers
|-- im
|-- im_v2
|-- infra
|-- market_data
|-- oms
|-- optimizer
`-- research_amp
```

### helpers

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" helpers 2>&1 | tee /tmp/tmp)

helpers
|-- README.md
|-- __init__.py
|-- build_helpers_package.sh
|-- cache.md
|-- cache.py
|-- csv_helpers.py
|-- dataframe.py
|-- datetime_.py
|-- dbg.py
|-- dict.py
|-- docker_manager.py
|-- env.py
|-- git.py
|-- hasyncio.py
|-- hnumpy.py
|-- hpandas.py
|-- hparquet.py
|-- htqdm.py
|-- htypes.py
|-- introspection.py
|-- io_.py
|-- joblib_helpers.md
|-- joblib_helpers.py
|-- jupyter.py
|-- lib_tasks.py
|-- list.py
|-- network.py
|-- numba_.py
|-- old
|   |-- __init__.py
|   |-- conda.py
|   |-- env2.py
|   |-- tunnels.py
|   `-- user_credentials.py
|-- open.py
|-- parser.py
|-- pickle_.py
|-- playback.md
|-- playback.py
|-- printing.py
|-- s3.py
|-- send_email.py
|-- sql.py
|-- system_interaction.py
|-- table.py
|-- telegram_notify
|   |-- README.md
|   |-- __init__.py
|   |-- config.py
|   |-- get_chat_id.py
|   `-- telegram_notify.py
|-- timer.py
|-- traceback_helper.py
|-- translate.py
|-- versioning.py
`-- warnings_helpers.py
```

### core

```test
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" core 2>&1 | tee /tmp/tmp)

core
|-- __init__.py
|-- architecture.md
|-- artificial_signal_generators.py
|-- bayesian.py
|-- config
|   |-- __init__.py
|   |-- builder.py
|   |-- config_.py
|   `-- utils.py
|-- covariance_shrinkage.py
|-- data_adapters.py
|-- event_study
|   |-- __init__.py
|   |-- core.py
|   `-- visualization.py
|-- explore.py
|-- feature_analyzer.py
|-- features.py
|-- finance.py
|-- information_bars
|   |-- __init__.py
|   `-- bars.py
|-- optimizer_baseline.py
|-- pandas_helpers.py
|-- plotting.py
|-- real_time.py
|-- real_time_example.py
|-- real_time_simple_model.py
|-- residualizer.py
|-- signal_processing.py
|-- statistics.py
`-- timeseries_study.py
```

### dataflow

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" dataflow 2>&1 | tee /tmp/tmp)

dataflow
|-- __init__.py
|-- core
|   |-- __init__.py
|   |-- dag_builder.py
|   |-- dag_builder_example.py
|   |-- dag.py
|   |-- dag_adapter.py
|   |-- node.py
|   |-- nodes
|   |   |-- __init__.py
|   |   |-- base.py
|   |   |-- gluonts_models.py
|   |   |-- local_level_model.py
|   |   |-- regression_models.py
|   |   |-- sarimax_models.py
|   |   |-- sinks.py
|   |   |-- sklearn_models.py
|   |   |-- sources.py
|   |   |-- transformers.py
|   |   |-- types.py
|   |   |-- unsupervised_sklearn_models.py
|   |   `-- volatility_models.py
|   |-- result_bundle.py
|   |-- dag_runner.py
|   |-- utils.py
|   |-- visitors.py
|   `-- visualization.py
|-- dataflow_design.md
|-- model
|   |-- __init__.py
|   |-- architecture.md
|   |-- dataframe_modeler.py
|   |-- incremental_single_name_model_evaluator.py
|   |-- master_experiment.py
|   |-- model_evaluator.py
|   |-- model_plotter.py
|   |-- regression_analyzer.py
|   |-- run_experiment.py
|   |-- run_experiment_stub.py
|   |-- run_prod_model_flow.py
|   |-- stats_computer.py
|   `-- utils.py
|-- pipelines
|   |-- __init__.py
|   |-- dataflow_example.py
|   |-- event_study
|   |   |-- __init__.py
|   |   `-- pipeline.py
|   |-- features
|   |   |-- __init__.py
|   |   `-- pipeline.py
|   |-- price
|   |   |-- __init__.py
|   |   `-- pipeline.py
|   |-- real_time
|   |   `-- __init__.py
|   `-- returns
|       |-- __init__.py
|       `-- pipeline.py
|-- scripts
|   `-- process_experiment_result.py
`-- system
    |-- __init__.py
    |-- real_time_dag_adapter.py
    |-- real_time_dag_runner.py
    |-- research_dag_adapter.py
    |-- sink_nodes.py
    `-- source_nodes.py
```

### im

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" im 2>&1 | tee /tmp/tmp)

im
|-- Makefile
|-- README.md
|-- __init__.py
|-- airflow
                                - TODO(gp): Obsolete
|-- app
                                - TODO(gp): Obsolete
|-- architecture.md
|-- ccxt
|   |-- __init__.py
|   |-- data
|   |   `-- __init__.py
|   `-- db
|       `-- __init__.py
|-- code_layout.md
|-- common
|   |-- __init__.py
|   |-- data
|   |   |-- __init__.py
|   |   |-- extract
|   |   |   |-- __init__.py
|   |   |   `-- data_extractor.py
|   |   |-- load
|   |   |   |-- __init__.py
|   |   |   |-- abstract_data_loader.py
|   |   |   `-- file_path_generator.pyj
|   |   |-- transform
|   |   |   |-- __init__.py
|   |   |   |-- s3_to_sql_transformer.py
|   |   |   `-- transform.py
|   |   `-- types.py
|   |-- metadata
|   |   |-- __init__.py
|   |   `-- symbols.py
|   `-- sql_writer.py
|-- cryptodatadownload
|   `-- data
|       |-- __init__.py
|       `-- load
|           |-- __init__.py
|           `-- loader.py
|-- devops.old
                                - TODO(gp): Obsolete
|-- devops.old2
                                - TODO(gp): Obsolete
|-- eoddata
|   |-- __init__.py
|   `-- metadata
|       |-- __init__.py
|       |-- extract
|       |   `-- download_symbol_list.py
|       |-- load
|       |   |-- __init__.py
|       |   `-- loader.py
|       `-- types.py
|-- ib
|-- kibot
                                - TODO(gp): Move to im_v2
```

### market_data

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" market_data 2>&1 | tee /tmp/tmp)

market_data
|-- __init__.py
|-- market_data_client.py
|-- market_data_client_example.py
|-- market_data_interface.py
`-- market_data_interface_example.py
```

### oms

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" oms 2>&1 | tee /tmp/tmp)

oms
|-- __init__.py
|-- api.py
|-- architecture.md
|-- broker.py
|-- broker_example.py
|-- call_optimizer.py
|-- devops
|   |-- __init__.py
    ...
|-- invoke.yaml
|-- locates.py
|-- oms_db.py
|-- oms_lib_tasks.py
|-- oms_utils.py
|-- order.py
|-- order_example.py
|-- order_processor.py
|-- pnl_simulator.py
|-- pnl_simulator.py.numba
|-- portfolio.py
|-- portfolio_example.py
|-- process_forecasts.py
`-- tasks.py
```

### research_amp

```text
(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" research_amp 2>&1 | tee /tmp/tmp)

research_amp
`-- cc
    |-- __init__.py
    |-- detect_outliers.py
    |-- statistics.py
    `-- volume.py
```

# All Python files

```text
(cd amp; tree -v --prune --charset=ascii -P "*.py" -I "*test*|*notebooks*" 2>&1 | tee /tmp/tmp)

.
|-- __init__.py
|-- core
|   |-- __init__.py
|   |-- artificial_signal_generators.py
|   |-- bayesian.py
|   |-- config
|   |   |-- __init__.py
|   |   |-- builder.py
|   |   |-- config_.py
|   |   `-- utils.py
|   |-- covariance_shrinkage.py
|   |-- data_adapters.py
|   |-- event_study
|   |   |-- __init__.py
|   |   |-- core.py
|   |   `-- visualization.py
|   |-- explore.py
|   |-- feature_analyzer.py
|   |-- features.py
|   |-- finance.py
|   |-- information_bars
|   |   |-- __init__.py
|   |   `-- bars.py
|   |-- optimizer_baseline.py
|   |-- pandas_helpers.py
|   |-- plotting.py
|   |-- real_time.py
|   |-- real_time_example.py
|   |-- real_time_simple_model.py
|   |-- residualizer.py
|   |-- signal_processing.py
|   |-- statistics.py
|   `-- timeseries_study.py
|-- dataflow
|   |-- __init__.py
|   |-- core
|   |   |-- __init__.py
|   |   |-- builders.py
|   |   |-- builders_example.py
|   |   |-- dag.py
|   |   |-- dag_adapter.py
|   |   |-- node.py
|   |   |-- nodes
|   |   |   |-- __init__.py
|   |   |   |-- base.py
|   |   |   |-- gluonts_models.py
|   |   |   |-- local_level_model.py
|   |   |   |-- regression_models.py
|   |   |   |-- sarimax_models.py
|   |   |   |-- sinks.py
|   |   |   |-- sklearn_models.py
|   |   |   |-- sources.py
|   |   |   |-- transformers.py
|   |   |   |-- types.py
|   |   |   |-- unsupervised_sklearn_models.py
|   |   |   `-- volatility_models.py
|   |   |-- result_bundle.py
|   |   |-- runners.py
|   |   |-- utils.py
|   |   |-- visitors.py
|   |   `-- visualization.py
|   |-- model
|   |   |-- __init__.py
|   |   |-- dataframe_modeler.py
|   |   |-- incremental_single_name_model_evaluator.py
|   |   |-- master_experiment.py
|   |   |-- model_evaluator.py
|   |   |-- model_plotter.py
|   |   |-- regression_analyzer.py
|   |   |-- run_experiment.py
|   |   |-- run_experiment_stub.py
|   |   |-- run_prod_model_flow.py
|   |   |-- stats_computer.py
|   |   `-- utils.py
|   |-- pipelines
|   |   |-- __init__.py
|   |   |-- dataflow_example.py
|   |   |-- event_study
|   |   |   |-- __init__.py
|   |   |   `-- pipeline.py
|   |   |-- features
|   |   |   |-- __init__.py
|   |   |   `-- pipeline.py
|   |   |-- price
|   |   |   |-- __init__.py
|   |   |   `-- pipeline.py
|   |   |-- real_time
|   |   |   `-- __init__.py
|   |   `-- returns
|   |       |-- __init__.py
|   |       `-- pipeline.py
|   |-- scripts
|   |   `-- process_experiment_result.py
|   `-- system
|       |-- __init__.py
|       |-- real_time_dag_adapter.py
|       |-- real_time_dag_runner.py
|       |-- research_dag_adapter.py
|       |-- sink_nodes.py
|       `-- source_nodes.py
|-- dev_scripts
|   |-- __init__.py
|   |-- aws
|   |   |-- AWS_dashboard.py
|   |   `-- am_aws.py
|   |-- compile_all.py
|   |-- diff_to_vimdiff.py
|   |-- email_notify.py
|   |-- ffind.py
|   |-- git
|   |   |-- __init__.py
|   |   |-- gd_notebook.py
|   |   |-- git_hooks
|   |   |   |-- __init__.py
|   |   |   |-- commit-msg.py
|   |   |   |-- install_hooks.py
|   |   |   |-- pre-commit-dry-run.py
|   |   |   |-- pre-commit.py
|   |   |   |-- translate.py
|   |   |   `-- utils.py
|   |   |-- git_submodules.py
|   |   |-- gsp.py
|   |   `-- gup.py
|   |-- grsync.py
|   |-- infra
|   |   |-- gdrive.py
|   |   `-- old
|   |       `-- ssh_tunnels.py
|   |-- manage_cache.py
|   |-- measure_import_times.py
|   |-- old
|   |   |-- create_conda
|   |   |   |-- _bootstrap.py
|   |   |   |-- _setenv_amp.py
|   |   |   |-- _setenv_lib.py
|   |   |   `-- install
|   |   |       |-- check_develop_packages.py
|   |   |       |-- create_conda.py
|   |   |       `-- print_conda_packages.py
|   |   `-- linter
|   |       |-- linter.py
|   |       |-- linter_master_report.py
|   |       |-- pre_pr_checklist.py
|   |       `-- process_jupytext.py
|   |-- parallel_script_skeleton.py
|   |-- process_prof.py
|   |-- remove_escape_chars.py
|   |-- replace_text.py
|   |-- script_skeleton.py
|   |-- string_to_file.py
|   |-- tg.py
|   |-- to_clean
|   |   |-- gen_utils.ORIG.py
|   |   `-- gen_utils.py
|   |-- toml_merge.py
|   |-- traceback_to_cfile.py
|   |-- transform_skeleton.py
|   |-- url.py
|   `-- zip_files.py
|-- documentation
|   |-- __init__.py
|   `-- scripts
|       |-- __init__.py
|       |-- convert_txt_to_pandoc.py
|       |-- generate_latex_sty.py
|       |-- generate_script_catalog.py
|       |-- lint_txt.py
|       |-- pandoc.py
|       |-- render_md.py
|       |-- replace_latex.py
|       `-- transform_txt.py
|-- helpers
|   |-- __init__.py
|   |-- cache.py
|   |-- csv_helpers.py
|   |-- dataframe.py
|   |-- datetime_.py
|   |-- dbg.py
|   |-- dict.py
|   |-- docker_manager.py
|   |-- env.py
|   |-- git.py
|   |-- hasyncio.py
|   |-- hnumpy.py
|   |-- hpandas.py
|   |-- hparquet.py
|   |-- htqdm.py
|   |-- htypes.py
|   |-- introspection.py
|   |-- io_.py
|   |-- joblib_helpers.py
|   |-- jupyter.py
|   |-- lib_tasks.py
|   |-- list.py
|   |-- network.py
|   |-- numba_.py
|   |-- old
|   |   |-- __init__.py
|   |   |-- conda.py
|   |   |-- env2.py
|   |   |-- tunnels.py
|   |   `-- user_credentials.py
|   |-- open.py
|   |-- parser.py
|   |-- pickle_.py
|   |-- playback.py
|   |-- printing.py
|   |-- s3.py
|   |-- send_email.py
|   |-- sql.py
|   |-- system_interaction.py
|   |-- table.py
|   |-- telegram_notify
|   |   |-- __init__.py
|   |   |-- config.py
|   |   |-- get_chat_id.py
|   |   `-- telegram_notify.py
|   |-- timer.py
|   |-- traceback_helper.py
|   |-- translate.py
|   |-- versioning.py
|   `-- warnings_helpers.py
|-- im
|   |-- __init__.py
|   |-- airflow
|   |   `-- devops
|   |       `-- dags
|   |           `-- im_infra.py
|   |-- app
|   |   |-- __init__.py
|   |   |-- print_db_connection.py
|   |   |-- services
|   |   |   |-- __init__.py
|   |   |   |-- file_path_generator_factory.py
|   |   |   |-- loader_factory.py
|   |   |   |-- sql_writer_factory.py
|   |   |   |-- symbol_universe_factory.py
|   |   |   `-- transformer_factory.py
|   |   `-- transform
|   |       |-- __init__.py
|   |       `-- convert_s3_to_sql.py
|   |-- ccxt
|   |   |-- __init__.py
|   |   |-- data
|   |   |   `-- __init__.py
|   |   `-- db
|   |       `-- __init__.py
|   |-- common
|   |   |-- __init__.py
|   |   |-- data
|   |   |   |-- __init__.py
|   |   |   |-- extract
|   |   |   |   |-- __init__.py
|   |   |   |   `-- data_extractor.py
|   |   |   |-- load
|   |   |   |   |-- __init__.py
|   |   |   |   |-- abstract_data_loader.py
|   |   |   |   `-- file_path_generator.py
|   |   |   |-- transform
|   |   |   |   |-- __init__.py
|   |   |   |   |-- s3_to_sql_transformer.py
|   |   |   |   `-- transform.py
|   |   |   `-- types.py
|   |   |-- metadata
|   |   |   |-- __init__.py
|   |   |   `-- symbols.py
|   |   `-- sql_writer.py
|   |-- cryptodatadownload
|   |   `-- data
|   |       |-- __init__.py
|   |       `-- load
|   |           |-- __init__.py
|   |           `-- loader.py
|   |-- devops.old
|   |   `-- docker_scripts
|   |       `-- init_im_db.py
|   |-- devops.old2
|   |   |-- __init__.py
|   |   `-- docker_scripts
|   |       |-- __init__.py
|   |       `-- set_schema_im_db.py
|   |-- eoddata
|   |   |-- __init__.py
|   |   `-- metadata
|   |       |-- __init__.py
|   |       |-- extract
|   |       |   `-- download_symbol_list.py
|   |       |-- load
|   |       |   |-- __init__.py
|   |       |   `-- loader.py
|   |       `-- types.py
|   |-- ib
|   |   |-- __init__.py
|   |   |-- connect
|   |   |   |-- devops
|   |   |   |   |-- docker_scripts
|   |   |   |   |   |-- make_ib_controller_init_file.py
|   |   |   |   |   `-- make_jts_init_file.py
|   |   |   |   `-- sanity_check_ib.py
|   |   |   `-- tasks.py
|   |   |-- data
|   |   |   |-- __init__.py
|   |   |   |-- config.py
|   |   |   |-- extract
|   |   |   |   |-- __init__.py
|   |   |   |   |-- download_ib_data.py
|   |   |   |   |-- gateway
|   |   |   |   |   |-- __init__.py
|   |   |   |   |   |-- download_benchmark.py
|   |   |   |   |   |-- download_data_ib_loop.py
|   |   |   |   |   |-- download_ib_data_single_file_with_loop.py
|   |   |   |   |   |-- download_realtime_data.py
|   |   |   |   |   |-- metadata.py
|   |   |   |   |   |-- save_historical_data_with_IB_loop.py
|   |   |   |   |   |-- scratch
|   |   |   |   |   |   |-- exercise_ib.py
|   |   |   |   |   |   |-- exercise_ibapi.py
|   |   |   |   |   |   `-- exercise_ibapi2.py
|   |   |   |   |   |-- unrolling_download_data_ib_loop.py
|   |   |   |   |   `-- utils.py
|   |   |   |   `-- ib_data_extractor.py
|   |   |   |-- load
|   |   |   |   |-- __init__.py
|   |   |   |   |-- ib_file_path_generator.py
|   |   |   |   |-- ib_s3_data_loader.py
|   |   |   |   `-- ib_sql_data_loader.py
|   |   |   `-- transform
|   |   |       |-- __init__.py
|   |   |       `-- ib_s3_to_sql_transformer.py
|   |   |-- metadata
|   |   |   |-- __init__.py
|   |   |   |-- extract
|   |   |   |   `-- ib_metadata_crawler
|   |   |   |       |-- __init__.py
|   |   |   |       |-- items.py
|   |   |   |       |-- middlewares.py
|   |   |   |       |-- pipelines.py
|   |   |   |       |-- settings.py
|   |   |   |       `-- spiders
|   |   |   |           |-- __init__.py
|   |   |   |           `-- ibroker.py
|   |   |   `-- ib_symbols.py
|   |   `-- sql_writer.py
|   `-- kibot
|       |-- __init__.py
|       |-- base
|       |   |-- __init__.py
|       |   `-- command.py
|       |-- data
|       |   |-- __init__.py
|       |   |-- config.py
|       |   |-- extract
|       |   |   |-- __init__.py
|       |   |   |-- check_realtime_feed.py
|       |   |   `-- download.py
|       |   |-- load
|       |   |   |-- __init__.py
|       |   |   |-- dataset_name_parser.py
|       |   |   |-- futures_forward_contracts.py
|       |   |   |-- kibot_file_path_generator.py
|       |   |   |-- kibot_s3_data_loader.py
|       |   |   `-- kibot_sql_data_loader.py
|       |   `-- transform
|       |       |-- __init__.py
|       |       |-- convert_s3_to_sql_kibot.py
|       |       `-- kibot_s3_to_sql_transformer.py
|       |-- metadata
|       |   |-- __init__.py
|       |   |-- config.py
|       |   |-- extract
|       |   |   |-- __init__.py
|       |   |   |-- download_adjustments.py
|       |   |   `-- download_ticker_lists.py
|       |   |-- load
|       |   |   |-- __init__.py
|       |   |   |-- adjustments.py
|       |   |   |-- contract_symbol_mapping.py
|       |   |   |-- expiry_contract_mapper.py
|       |   |   |-- kibot_metadata.py
|       |   |   |-- s3_backend.py
|       |   |   `-- ticker_lists.py
|       |   `-- types.py
|       `-- sql_writer.py
|-- im_v2
|   |-- __init__.py
|   |-- ccxt
|   |   |-- data
|   |   |   |-- client
|   |   |   |   |-- __init__.py
|   |   |   |   |-- ccx_clients_example.py
|   |   |   |   `-- clients.py
|   |   |   `-- extract
|   |   |       |-- __init__.py
|   |   |       |-- dags
|   |   |       |   `-- rt_dag.py
|   |   |       |-- download_historical.py
|   |   |       |-- download_realtime.py
|   |   |       `-- exchange_class.py
|   |   |-- db
|   |   |   |-- __init__.py
|   |   |   `-- utils.py
|   |   `-- universe
|   |       |-- __init__.py
|   |       `-- universe.py
|   |-- common
|   |   |-- __init__.py
|   |   |-- data
|   |   |   |-- __init__.py
|   |   |   |-- client
|   |   |   |   |-- __init__.py
|   |   |   |   `-- clients.py
|   |   |   `-- transform
|   |   |       |-- __init__.py
|   |   |       |-- convert_pq_by_date_to_by_asset.py
|   |   |       |-- csv_to_pq.py
|   |   |       |-- extract_data_from_db.py
|   |   |       |-- pq_convert.py
|   |   |       `-- utils.py
|   |   `-- db
|   |       |-- __init__.py
|   |       |-- create_db.py
|   |       |-- remove_db.py
|   |       `-- utils.py
|   |-- cryptodatadownload
|   |   `-- data
|   |       `-- extract
|   |           `-- download_historical.py
|   |-- devops
|   |   `-- __init__.py
|   |-- im_lib_tasks.py
|   `-- tasks.py
|-- infra
|   |-- __init__.py
|   `-- scripts
|       |-- __init__.py
|       `-- aws
|           |-- __init__.py
|           |-- aws_manager.py
|           `-- tasks.py
|-- market_data
|   |-- __init__.py
|   |-- market_data_client.py
|   |-- market_data_client_example.py
|   |-- market_data_interface.py
|   `-- market_data_interface_example.py
|-- oms
|   |-- __init__.py
|   |-- api.py
|   |-- broker.py
|   |-- broker_example.py
|   |-- call_optimizer.py
|   |-- devops
|   |   |-- __init__.py
|   |   `-- docker_scripts
|   |       `-- __init__.py
|   |-- locates.py
|   |-- oms_db.py
|   |-- oms_lib_tasks.py
|   |-- oms_utils.py
|   |-- order.py
|   |-- order_example.py
|   |-- order_processor.py
|   |-- pnl_simulator.py
|   |-- portfolio.py
|   |-- portfolio_example.py
|   |-- process_forecasts.py
|   `-- tasks.py
|-- optimizer
|   |-- __init__.py
|   |-- base.py
|   |-- constraints.py
|   |-- costs.py
|   |-- repo_config.py
|   |-- single_period_optimization.py
|   |-- tasks.py
|   `-- utils.py
|-- repo_config.py
|-- research_amp
|   `-- cc
|       |-- __init__.py
|       |-- detect_outliers.py
|       |-- statistics.py
|       `-- volume.py
|-- setup.py
`-- tasks.py
```

# Invariants

- We assume that there is no file with the same name either in the same repo or
  across different repos
  - In case of name collision, we prepend as many dirs as necessary to make the
    filename unique
  - E.g., the files below should be renamed:

    ```bash
    > ffind.py utils.py | grep -v test
    ./amp/core/config/utils.py
      -> amp/core/config/config_utils.py

    ./amp/dataflow/core/utils.py
      -> amp/dataflow/core_config.py

    ./amp/dataflow/model/utils.py
      -> amp/dataflow/model/model_utils.py
    ```
  - Note that this rule makes the naming of files depending on the history, but
    it minimizes churn of names

# Misc

- To execute a vim command, go on the line

  ```bash
  :exec '!'.getline('.')
  :read /tmp/tmp
  ```

- To inline in vim

  ```bash
  !(cd amp; tree -v --charset=ascii -I "*test*|*notebooks*" market_data 2>&1 | tee /tmp/tmp)
  :read /tmp/tmp
  ```

- Print only dirs

  ```bash
  > tree -d
  ```

- Print only dirs up to a certain level

  ```bash
  > tree -L 1 -d
  ```

- Sort alphanumerically

  ```bash
  > tree -v
  ```

- Print full name so that one can also grep

  ```bash
  > tree -v -f --charset=ascii -I "*test*|*notebooks*" | grep amp | grep -v dev_scripts

  `-- dataflow/system
      |-- dataflow/system/__init__.py
      |-- dataflow/system/real_time_dag_adapter.py
      |-- dataflow/system/real_time_dag_runner.py
      |-- dataflow/system/research_dag_adapter.py
      |-- dataflow/system/sink_nodes.py
      `-- dataflow/system/source_nodes.py
  ```
