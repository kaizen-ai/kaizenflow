

<!-- toc -->

- [Files](#files)
- [Current dir structure](#current-dir-structure)
  * [Meta](#meta)
  * [Processes](#processes)
  * [Software components](#software-components)
  * [Papers](#papers)
- [How to organize the docs](#how-to-organize-the-docs)
  * [Dir vs no-dirs](#dir-vs-no-dirs)
  * [Tracking reviews and improvements](#tracking-reviews-and-improvements)
  * [How to search the documentation](#how-to-search-the-documentation)
  * [How to ensure that all the docs are cross-referenced in the indices?](#how-to-ensure-that-all-the-docs-are-cross-referenced-in-the-indices)
- [List of files](#list-of-files)

<!-- tocstop -->

# Files

- `docs/meta.md`
  - This file contains rules and conventions for all the documentation under
    `docs`.

- `docs/all.code_organization.reference.md`
  - Describe how the code is organized in terms of components, libraries, and
    directories

- `docs/all.software_components.reference.md`
  - List of all the software components in the codebase

- `docs/all.workflow.explanation.md`
  - Describe all the workflows for quants, quant devs, and devops

# Current dir structure

Please keep the directory in a conceptual order.

The current dir structure of `docs` is:

## Meta

- `documentation_meta`
  - How to write documentation for code and workflows.

## Processes

- `onboarding`
  - Practicalities of on-boarding new team members.
  - E.g., things typically done only once at the beginning of joining the team.

- `work_organization`
  - How the work is organized on a general level
  - E.g., the company's adopted practices spanning coding and development

- `work_tools`
  - How to set up, run and use various software needed for development
  - E.g., IDE

- `general_background`
  - Documents that provide general reference information, often across different
    topics (e.g., glossaries, reading lists).

- `coding`
  - Guidelines and good practices for coding and code-adjacent activities (such
    as code review)
  - This includes general tips and tricks that are useful for anybody writing
    any code (e.g., how to use type hints) as well as in-depth descriptions of
    specific functions and libraries.

## Software components

- `kaizenflow`
  - Docs related to high-level packages that are used across the codebase (e.g.,
    `helpers`, `config`), as well as overall codebase organization.

- `datapull`
  - Docs related to dataset handling: downloading, onboarding, interpretation,
    etc.

- `dataflow`
  - Docs related to the framework of implementing and running machine learning
    models.

- `trade_execution`
  - Docs related to placing and monitoring trading orders to market or broker.

- `infra`
  - Docs related to the company’s infrastructure: AWS services, code deployment,
    monitoring, server administration, etc.

## Papers

- `papers`
  - Papers written by the team members about the company's products and
    know-how.

# How to organize the docs

- Documentation can be organized in multiple ways:
  - By software component
  - By functionality (e.g., infra, backtesting)
  - By team (e.g., trading ops)

- We have decided that
  - For each software component there should be a corresponding documentation
  - We have documentation for each functionality and team

## Dir vs no-dirs

- Directories make it difficult to navigate the docs
- We use “name spaces” until we have enough objects to create a dir

##

- Doc needs to be reviewed "actively", e.g., by making sure someone checks them
  in the field
- Somebody should verify that is "executable"

## Tracking reviews and improvements

- There is a
  [Master Documentation Gdoc](https://docs.google.com/document/d/1sEG5vGkaNIuMEkCHgkpENTUYxDgw1kZXb92vCw53hO4)
  that contains a list of tasks related to documentation, including what needs
  to be reviewed

- For small action items we add a markdown TODO like we do for the code
  ```
  <!-- TODO(gp): ... -->
  ```

- To track the last revision we use a tag at the end of the document like:
  ```markdown
  Last review: GP on 2024-04-20, ...
  ```

## How to search the documentation

- Be patient and assume that the documentation is there, but you can't find it
  because you are not familiar with it and not because you think the
  documentation is poorly done or not organized

- Look for files that contain words related to what you are looking for
  - E.g., `ffind.py XYZ`
- Grep in the documentation looking for words related to what you are looking
  for
  - E.g., `jackmd trading`
- Scan through the content of the references
  - E.g., `all.code_organization.reference.md`
- Grep for the name of a tool in the documentation

## How to ensure that all the docs are cross-referenced in the indices?

- There is a script to check and update the documentation cross-referencing
  files in a directory and a file with all the links to the files

/Users/saggese/src/dev_tools1/linters/amp_fix_md_links.py
docs/all.amp_fix_md_links.explanation.md

- How to point mistakes in a md file?

# List of files
```
> tree docs -I '*figs*|test*' --dirsfirst -n -F --charset unicode | grep -v __init__.py
```

The current structure of files are:
```
docs/
|-- build/
|   |-- all.linter_gh_workflow.explanation.md
|   |-- all.pytest_allure.explanation.md
|   |-- all.pytest_allure.how_to_guide.md
|   |-- all.semgrep_workflow.explanation.md
|   |-- ck.gh_workflows.explanation.md
|   `-- ck.gh_workflows.reference.md
|-- coding/
|   |-- all.asyncio.explanation.md
|   |-- all.code_design.how_to_guide.md
|   |-- all.code_like_pragmatic_programmer.how_to_guide.md
|   |-- all.code_review.how_to_guide.md
|   |-- all.coding_style.how_to_guide.md
|   |-- all.gsheet_into_pandas.how_to_guide.md
|   |-- all.hcache.explanation.md
|   |-- all.hgoogle_file_api.explanation.md
|   |-- all.hplayback.how_to_guide.md
|   |-- all.imports_and_packages.how_to_guide.md
|   |-- all.integrate_repos.how_to_guide.md
|   |-- all.jupyter_notebook.how_to_guide.md
|   |-- all.plotting.how_to_guide.md
|   |-- all.profiling.how_to_guide.md
|   |-- all.publish_notebook.how_to_guide.md
|   |-- all.qgrid.how_to_guide.md
|   |-- all.reading_other_people_code.how_to_guide.md
|   |-- all.run_unit_tests.how_to_guide.md
|   |-- all.str_to_df.how_to_guide.md
|   |-- all.submit_code_for_review.how_to_guide.md
|   |-- all.type_hints.how_to_guide.md
|   `-- all.write_unit_tests.how_to_guide.md
|-- dataflow/
|   |-- system/
|   |   |-- all.use_system_config.tutorial.ipynb
|   |   |-- all.use_system_config.tutorial.py
|   |   |-- ck.build_real_time_dag.explanation.md
|   |   |-- ck.build_real_time_dag.tutorial.ipynb
|   |   `-- ck.build_real_time_dag.tutorial.py
|   |-- all.batch_and_streaming_mode_using_tiling.explanation.md
|   |-- all.best_practice_for_building_dags.explanation.md
|   |-- all.build_first_dag.tutorial.ipynb
|   |-- all.build_first_dag.tutorial.py
|   |-- all.build_simple_risk_model_dag.tutorial.ipynb
|   |-- all.build_simple_risk_model_dag.tutorial.py
|   |-- all.computation_as_graphs.explanation.md
|   |-- all.dag.explanation.md
|   |-- all.dataflow.explanation.md
|   |-- all.dataflow_data_format.explanation.md
|   |-- all.simulation_output.reference.md
|   |-- all.time_series.explanation.md
|   |-- all.timing_semantic_and_clocks.md
|   |-- all.train_and_predict_phases.explanation.md
|   |-- ck.data_pipeline_architecture.reference.md
|   |-- ck.event_study.explanation.md
|   |-- ck.export_alpha_data.explanation.md
|   |-- ck.export_alpha_data.how_to_guide.md
|   |-- ck.load_alpha_and_trades.tutorial.ipynb
|   |-- ck.load_alpha_and_trades.tutorial.py
|   |-- ck.master_notebooks.reference.md
|   |-- ck.release_encrypted_models.explanation.md
|   |-- ck.release_encrypted_models.how_to_guide.md
|   |-- ck.research_methodology.explanation.md
|   |-- ck.run_backtest.explanation.md
|   |-- ck.run_backtest.how_to_guide.md
|   |-- ck.run_batch_computation_dag.explanation.md
|   |-- ck.run_batch_computation_dag.tutorial.ipynb
|   |-- ck.run_batch_computation_dag.tutorial.py
|   `-- ck.system_design.explanation.md
|-- datapull/
|   |-- all.datapull_client_stack.explanation.md
|   |-- all.datapull_derived_data.explanation.md
|   |-- all.datapull_qa_flow.explanation.md
|   |-- all.datapull_sandbox.explanation.md
|   |-- all.dataset_onboarding_checklist.reference.md
|   |-- all.im_client.reference.ipynb
|   |-- all.im_client.reference.py
|   |-- all.market_data.reference.ipynb
|   |-- all.market_data.reference.py
|   |-- all.update_CCXT_version.how_to_guide.md
|   |-- ck.add_new_data_source.how_to_guide.md
|   |-- ck.binance_bid_ask_data_pipeline.explanation.md
|   |-- ck.binance_ohlcv_data_pipeline.explanation.md
|   |-- ck.binance_trades_data_pipeline.explanation.md
|   |-- ck.ccxt_exchange_timestamp_interpretation.reference.md
|   |-- ck.create_airflow_dag.tutorial.md
|   |-- ck.datapull.explanation.md
|   |-- ck.datapull_data_quality_assurance.reference.md
|   |-- ck.develop_an_airflow_dag_for_production.explanation.md
|   |-- ck.handle_datasets.how_to_guide.md
|   |-- ck.kibot_data.explanation.md
|   |-- ck.kibot_timing.reference.md
|   |-- ck.onboarding_new_exchnage.md
|   |-- ck.process_historical_data_without_dataflow.tutorial.ipynb
|   |-- ck.process_historical_data_without_dataflow.tutorial.py
|   |-- ck.run_ib_connect.how_to_guide.md
|   |-- ck.run_im_app.how_to_guide.md
|   |-- ck.universe.explanation.md
|   `-- ck.use_ib_metadata_crawler.how_to_guide.md
|-- deploying/
|   |-- all.model_deployment.how_to_guide.md
|   |-- ck.run_production_system.how_to_guide.md
|   `-- ck.supported_models.reference.md
|-- dev_tools/
|   `-- thin_env/
|       `-- all.gh_and_thin_env_requirements.reference.md
|-- documentation_meta/
|   |-- all.architecture_diagrams.explanation.md
|   |-- all.diataxis.explanation.md
|   |-- all.writing_docs.how_to_guide.md
|   `-- plotting_in_latex.how_to_guide.md
|-- general_background/
|   |-- all.common_abbreviations.reference.md
|   |-- all.glossary.reference.md
|   |-- all.literature_review.reference.md
|   |-- all.reading_list.reference.md
|   `-- ck.mailing_groups.reference.md
|-- infra/
|   |-- all.auto_scaling.explanation.md
|   |-- all.aws_scripts.reference.md
|   |-- all.rds.comparison.md
|   |-- ck.aws_api_key_rotation.how_to_guide.md
|   |-- ck.aws_elastic_file_system.explanation.md
|   |-- ck.create_client_vpn_endpoint.how_to_guide.md
|   |-- ck.development_stages.explanation.md
|   |-- ck.ec2_servers.explanation.md
|   |-- ck.kaizen_infrastructure.reference.md
|   |-- ck.s3_buckets.explanation.md
|   |-- ck.services.reference.md
|   |-- ck.set_up_aws_client_vpn.how_to_guide.md
|   |-- ck.set_up_s3_buckets.how_to_guide.md
|   |-- ck.set_up_utility_server_app.how_to_guide.md
|   |-- ck.sorrentum_experiments_aws_infrastructure.explanation.md
|   |-- ck.storing_secrets.explanation.md
|   `-- ck.use_htmlcov_server.how_to_guide.md
|-- kaizenflow/
|   |-- all.analyze_Mock2_pipeline_simulation.how_to_guide.ipynb
|   |-- all.analyze_Mock2_pipeline_simulation.how_to_guide.py
|   |-- all.dev_scripts_catalogue.reference.md
|   |-- all.devops_code_organization.reference.md
|   |-- all.install_helpers.how_to_guide.md
|   |-- all.run_Mock2_in_batch_mode.how_to_guide.md
|   |-- all.run_Mock2_pipeline_in_notebook.how_to_guide.ipynb
|   |-- all.run_Mock2_pipeline_in_notebook.how_to_guide.py
|   |-- all.run_end_to_end_Mock2_system.tutorial.md
|   |-- ck.config.explanation.md
|   `-- ck.system_config.explanation.md
|-- monitoring/
|   |-- ck.monitor_system.how_to_guide.md
|   |-- ck.system_reconciliation.explanation.md
|   `-- ck.system_reconciliation.how_to_guide.md
|-- onboarding/
|   |-- admin.onboarding_process.md
|   |-- all.communicate_in_telegram.how_to_guide.md
|   |-- all.development_documents.reference.md
|   |-- all.organize_email.how_to_guide.md*
|   |-- all.receive_crypto_payment.how_to_guide.md
|   |-- all.track_time_with_hubstaff.how_to_guide.md
|   |-- ck.development_setup.how_to_guide.md
|   |-- ck.setup_vpn_and_dev_server_access.how_to_guide.md
|   |-- sorrentum.hiring_meister.how_to_guide.md
|   |-- sorrentum.set_up_development_environment.how_to_guide.md
|   `-- sorrentum.signing_up.how_to_guide.md
|-- oms/
|   |-- broker/
|   |   |-- ck.ccxt_broker_logs_schema.reference.md
|   |   |-- ck.generate_broker_test_data.how_to_guide.md
|   |   |-- ck.replayed_ccxt_exchange.explanation.md
|   |   |-- ck.binance_terms.reference.md
|   |-- child_order_quantity_computer/
|   |-- limit_price_computer/
|   |-- order/
|   |-- ck.oms.explanation.md
|   |-- ck.execution_notebooks.explanation.md
|-- trade_execution/
|   |-- ccxt_notes.txt
|   |-- ck.git_branches.reference.md
|   |-- ck.model_configurations.reference.md
|   |-- ck.trade_execution_experiment.how_to_guide.md
|   |-- ck.trading.how_to_guide.md
|   |-- ck.trading.onboarding_new_binance_trading_account.how_to_guide.ipynb
|   `-- ck.trading.onboarding_new_binance_trading_account.how_to_guide.py
|-- work_organization/
|   |-- all.buildmeister.how_to_guide.md
|   |-- all.contributor_scoring.how_to_guide.md
|   |-- all.epicmeister.how_to_guide.md
|   |-- all.rollout.how_to_guide.md
|   |-- all.scrum.explanation.md
|   |-- all.team_collaboration.how_to_guide.md
|   |-- all.use_github_and_zenhub.how_to_guide.md
|   |-- ck.roles_and_responsibilities.md
|   `-- sorrentum.organize_your_work.how_to_guide.md
|-- work_tools/
|   |-- all.add_toc_to_notebook.how_to_guide.md
|   |-- all.bfg_repo_cleaner.how_to_guide.md
|   |-- all.chatgpt_api.how_to_guide.md
|   |-- all.codebase_clean_up.how_to_guide.md
|   |-- all.conda_environment_obsolete.how_to_guide.md
|   |-- all.development.how_to_guide.md
|   |-- all.docker.how_to_guide.md
|   |-- all.dockerhub.how_to_guide.md
|   |-- all.git.how_to_guide.md
|   |-- all.invoke_workflows.how_to_guide.md
|   |-- all.jupytext.how_to_guide.md
|   |-- all.latex.how_to_guide.md
|   |-- all.parquet.explanation.md
|   |-- all.pycharm.how_to_guide.md
|   |-- all.ssh.how_to_guide.md
|   |-- all.telegram_notify_bot.how_to_guide.md
|   `-- all.visual_studio_code.how_to_guide.md
|-- all.code_organization.reference.md
|-- all.components.reference.md
|-- all.software_components.reference.md
|-- all.workflow.explanation.md
`-- meta.md

18 directories, 204 files
```

Last review: GP on 2024-04-29
