

<!-- toc -->

- [Production system](#production-system)
  * [Run modes](#run-modes)
  * [How to run](#how-to-run)
  * [Output format](#output-format)
  * [Simulated production system](#simulated-production-system)
  * [How to analyze results](#how-to-analyze-results)
  * [Airflow scheduling](#airflow-scheduling)
    + [Dirs format](#dirs-format)
  * [PnL for investors](#pnl-for-investors)
  * [Get historical PnL graphs](#get-historical-pnl-graphs)

<!-- tocstop -->

# Production system

- Production system is a system that:
  - Is defined by a System config
  - Computes predictions given a model
  - Generates target positions given predictions
  - Submits orders to an exchange
  - Uses a wall clock to introduce time semantics
    - I.e. data is available only if it is known before a wall clock time to
      avoid future peaking

## Run modes

1. Production (`prod`)
   - Wall clock is the real wall clock, i.e. is not simulated
   - Data source is a production database which is updated in real-time
   - Broker places orders to an exchange (e.g., binance)
2. Paper trading (`paper_trading`)
   - Wall clock is the real wall clock, i.e. is not simulated
   - Data source is a production database which is updated in real-time
   - Broker is mocked, i.e. there is no interaction with an exchange assuming
     100% fill rate

## How to run

- To run a production system use
  `dataflow_orange/system/Cx/run_Cx_prod_system.sh`. Specify:
  - A model (e.g., `C1b`)
  - A run mode (e.g., `prod`)
  - For how long in seconds to run
  - Where to save the results

## Output format

The output of a production system run is a system log directory.

The structure of a system log directory is:

- System config which contains all the parameters that control the production
  system run
- DAG output, i.e. result of each DAG node per bar
  - DAG profiling stats, i.e. info about time and memory
- Portfolio output
  - Holdings (shares and notional)
  - Executed trades (shares and notional)
  - Statistics (e.g., PnL)
- Generated target positions
- Submitted orders

## Simulated production system

- Simulation is a special production system run mode that allows to test a
  real-time system replaying current times in the past:
  - Wall clock time is simulated, i.e. current time is remapped to a specified
    timestamp in the past
  - Data source is a file with a historical snapshot of market data
  - Broker is mocked, i.e. there is no interaction with an exchange assuming
    100% fill rate
- To run a simulation of a real-time system:
  - Dump a snapshot of market data to replay using
    `cmamp/dataflow_amp/system/Cx/Cx_dump_market_data.py`. Specify
    - A desired date range
    - A dir to save the file with market data
  - Run a production system in the simulation mode using a
    [template](https://github.com/cryptokaizen/cmamp/blob/master/dataflow_amp/system/Cx/scripts/Cx_template.run_prod_system_simulation.py).
    Specify
    - A desired date range
    - A model
    - A path to the file with market data to replay

## How to analyze results

- Run the notebook `cmamp/oms/notebooks/Master_PnL_real_time_observer.ipynb` and
  specify:
  - A model (e.g., `C1b`)
  - A desired date range
  - A root directory that contains the prod system output
- The notebook has the following structure:
  - Build the config that controls the notebook parameters
  - Load the DAG output
  - Compute execution time stats
  - Load production system Portfolio
  - Compute research Portfolio equivalent
  - Compare Portfolios
    - Production vs research

The latest PnL observer notebook for each model is published to the S3 HTML
bucket containing the prod system results for the last 5 minutes. The notebook
is updated every 5 minutes.

The link has the following format:
`http://172.30.2.44/system_reconciliation/{dag_builder_name}.last_5minutes.html`,
e.g., http://172.30.2.44/system_reconciliation/C1b.last_5minutes.html

## Airflow scheduling

Every available model has a scheduled DAG that runs the production system in the
paper trading every day for ~24 hours. Refer to the list of available models:
https://github.com/cryptokaizen/cmamp/blob/master/docs/deploying/ck.supported_models.reference.md

### Dirs format

- Current state:
  - Assumption: it’s extremely rare that 2 runs with different
    `airflow_dag_run_mode` or `prod_system_run_mode` have the exact same
    start-end timestamps.
    ```
    {shared_folder}/ecs/{stage}/{data_root_dir}/{dag_builder_name}/{prod_system_run_mode}/{start_timestamp}.{end_timestamp} \
        logs \
            log.{airflow_dag_run_mode}.txt
        pnl_realtime_observer_notebook \
            {run_timestamp} \
                …
        system_log_dir.{airflow_dag_run_mode} \
            …
    ```
  - E.g.:
    ```
    /data/shared/ecs/preprod/system_reconciliation/C3a/paper_trading/20230626_131000.20230627_130500 \
        logs \
            log.scheduled.txt
        pnl_realtime_observer_notebook \
            20230626_131000 \
                …
        system_log_dir.scheduled \
    ```

- More problems to solve:
  - Add `prod_system_run_mode` to the dir structure. The problem is that paper
    trading runs and prod runs are stored under different `data_root_dir`;
    instead would be better to create a subdir with a `prod_system_run_mode`
    - E.g., paper trading runs are stored under `system_reconciliation` and prod
      runs are stored under `twap_experiment`
  - Add `airflow_dag_run_mode` to the dir structure, now the information is
    encoded in `system_log_dir`,
    - E.g,. `system_log_dir.scheduled`
  - If possible save a log file directly under system_log_dir
    - No need to have a separate folder `logs`

The trade-off is: should we encode timestamp, run mode information in
`system_log_dir` name, e.g.,
`system_log_dir.scheduled.20230626_131000.20230627_130500` or just create the
corresponding subdirectories at higher level, e.g.,
`…/scheduled/0230626_131000.20230627_130500/system_log_dir`

Encoding in `system_log_dir` name works fine until there is only one entity
(file or dir) that corresponds, but if we had more than one file we would need
to rename all using the pattern.

So creating subdirectories at a higher level seems more general.

Target state:

- Note: `data_root_dir` is now the same for both paper trading and prod runs. To
  distinguish between those we use a subdir `prod_system_run_mode`.
```
{shared_folder}/ecs/{stage}/{data_root_dir}/{dag_builder_name}/{prod_system_run_mode}/{airflow_dag_run_mode}/{start_timestamp}.{end_timestamp} \
    pnl_realtime_observer_notebook \
        {run_timestamp} \
            …
    system_log_dir \
        …
        log.txt
```

- E.g.:
```
/data/shared/ecs/preprod/prod_system/C3a/paper_trading/scheduled/20230626_131000.20230627_130500 \
    pnl_realtime_observer_notebook \
        20230626_131000 \
            …
    system_log_dir \
        …
        log.txt
```

## PnL for investors

- Production PnL is published for investors.
  - Only for the C3a model
  - Cumulative PnL in $
- The results are stored under `s3://cryptokaizen-html/pnl_for_investors` and
  can be accessed using the public S3 bucket
  `https://cryptokaizen-public.s3.eu-north-1.amazonaws.com` mapped to the
  results dir
- There are 4 plots:
  - Since the inception:
    - https://cryptokaizen-public.s3.eu-north-1.amazonaws.com/pnl_for_investors/cumulative_pnl.since_2019.png
    - Generated once by
      `orange/dataflow_orange/pipelines/C3/notebooks/CmTask4039.historical_pnl_for_investors.ipynb`
  - Since 2022:
    - https://cryptokaizen-public.s3.eu-north-1.amazonaws.com/pnl_for_investors/cumulative_pnl.since_2022.png
    - Generated once by
      `orange/dataflow_orange/pipelines/C3/notebooks/CmTask4039.historical_pnl_for_investors.ipynb`
  - For the last 24 hours (updated daily):
    - https://cryptokaizen-public.s3.eu-north-1.amazonaws.com/pnl_for_investors/cumulative_pnl.last_24hours.png
    - Generated by `cmamp/oms/notebooks/Master_PnL_real_time_observer.ipynb`
      which is scheduled via AirFlow using
      `run_master_pnl_real_time_observer_notebook()` from
      `cmamp/oms/lib_tasks_reconcile.py`
  - For the last 5 minutes (updated every 5 minutes):
    - https://cryptokaizen-public.s3.eu-north-1.amazonaws.com/pnl_for_investors/cumulative_pnl.last_5minutes.png
    - Generated by `cmamp/oms/notebooks/Master_PnL_real_time_observer.ipynb`
      which is scheduled via AirFlow using
      `run_master_pnl_real_time_observer_notebook()` from
      `cmamp/oms/lib_tasks_reconcile.py`

## Get historical PnL graphs

- Run the script with these params
  - `DAG_BUILDER_NAME="C3a"`
  - `DAG_BUILDER_CTOR_AS_STR="dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"`
  - `BACKTEST_CONFIG="ccxt_v7_4-all.5T.2019-09-01_2023-05-09"` (change
    "2023-05-09" to the current day when running)
  - `FIT_AT_BEGINNING=None`
  - `TRAIN_TEST_MODE="ins"`
  - Run from a pre-prod branch
  - Script:
    https://github.com/cryptokaizen/cmamp/blob/master/dataflow_amp/system/Cx/run_Cx_historical_simulation.sh
- Copy the results to `/data/shared/model/historical`, e.g.,
  `/data/shared/model/historical/build_tile_configs.C3a.ccxt_v7_4-all.5T.2019-09-01_2023-03-10…`
- Run the notebook that generates the plots:
  - The notebook is:
    https://github.com/cryptokaizen/orange/blob/master/dataflow_orange/pipelines/C3/notebooks/CmTask4039.historical_pnl_for_investors.ipynb
  - Update `dir_name` in the config
  - Change `end_date` to the current date in the config
  - In `plot_cumulative_pnl()` temporary set `copy_to_s3: bool = False` so that
    we do not overwrite the existing files
  - Run the notebook end-to-end
  - Post 2 plots here: since 2019 and since 2022
- Publish the notebook by running:
  ```
  publish_notebook.py --file dataflow_orange/pipelines/C3/notebooks/CmTask4039.historical_pnl_for_investors.ipynb --action publish --aws_profile am
  ```
