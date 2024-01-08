<!--ts-->
   * [Analyze data](#analyze-data)
      * [Master_universe_analysis](#master_universe_analysis)
      * [Master_universe](#master_universe)
      * [Master_raw_data_gallery](#master_raw_data_gallery)
      * [Master_IM_DB](#master_im_db)
      * [Master_single_vendor_qa](#master_single_vendor_qa)
            * [Master_cross_vendor_qa](#master_cross_vendor_qa)
   * [Analyze features](#analyze-features)
      * [Master_tradability_analysis](#master_tradability_analysis)
      * [Master_crypto_analysis](#master_crypto_analysis)
   * [Run backtests](#run-backtests)
      * [Master_DAG_debugger](#master_dag_debugger)
   * [Post-process backtests](#post-process-backtests)
      * [Master_model_mixer](#master_model_mixer)
      * [Master_feature_analyzer](#master_feature_analyzer)
      * [Master_model_analyzer](#master_model_analyzer)
      * [Master_local_metrics](#master_local_metrics)
      * [Master_research_backtest_analyzer](#master_research_backtest_analyzer)
      * [Master_forecast_processor_reader](#master_forecast_processor_reader)
      * [Master_machine_learning](#master_machine_learning)
      * [Master_A_B_backtest_analyzer](#master_a_b_backtest_analyzer)
      * [Master_save_pnl_and_trades](#master_save_pnl_and_trades)
   * [Live trading](#live-trading)
      * [Master_portfolio_vs_research_stats](#master_portfolio_vs_research_stats)
      * [Master_system_run_debugger](#master_system_run_debugger)
   * [Execution analysis](#execution-analysis)
      * [Master_execution_analysis](#master_execution_analysis)
      * [Master_broker_debugging](#master_broker_debugging)
   * [Multi-day system reconciliation](#multi-day-system-reconciliation)
   * [Backtesting](#backtesting)
      * [Per-asset backtesting](#per-asset-backtesting)
            * [Basic concepts](#basic-concepts)



<!--te-->
Please maintain the notebooks in a logical order from raw data (e.g., from
source provider) to feature computation, to simulation

The notebooks can be open locally with:

```bash
> FILE=im/app/notebooks/Master_IM_DB.ipynb
> publish_notebook.py --file $FILE --action convert
```

# Analyze data

## Master_universe_analysis

File:

- [im_v2/common/universe/notebooks/Master_universe_analysis.ipynb]()

Description:

- Load data given a specific universe version
- Compute basic statistics, e.g., universe size, availability period, volume per
  asset

Example:

- `http://172.30.2.44/notebooks/Master_universe_analysis.20230705-095826.html`

## Master_universe

File:

- [im_v2/ccxt/notebooks/Master_universe.ipynb]()

Description:

- Load CCXT trade universes as full symbols and as asset ids

Example:

- `s3://cryptokaizen-html/notebooks/Master_universe.20230620-082431.html`

## Master_raw_data_gallery

File:

- [im_v2/common/notebooks/Master_raw_data_gallery.ipynb]()

Description:

- Show how to load the raw data from S3 and from DB for all the data sources we
  have available
  - See infra/S3/S3_buckets_overview.explanation.md
  - E.g., CCXT, CryptoChassis
- It should load the data for each data set we have available
  - No need to load different snapshots, but it should be parameterized with
    respect to it
- The notebook is organized using the same format that we use for storing data
  - `{download_mode}.{downloading_entity}.{data_format}/{snapshot}/{data_set}/{vendor}/{exchange}/{asset}`
- The notebook should run relatively quickly (we just want to make sure the data
  is available)

## Master_IM_DB

File:

- [im/app/notebooks/Master_IM_DB.ipynb]()

Description:

- Connect to a real-time IM DB and do some one-off analysis
- Need Postgres parameters as inputs
- TODO(Danya): See if we can use the notebook. The goal is worthy but the
  notebook is super-obsolete (P1)

Example:

- `s3://alphamatic-data/notebooks/Master_IM_DB.20220608-094416.html`

## Master_single_vendor_qa

File:

- [research_amp/cc/notebooks/Master_single_vendor_qa.ipynb]()

Description:

- Uses an `ImClient` to load data from S3
- Perform QA in order to exclude bad quality data from further use
- Compute and plot stats about:
  - Available universe
  - Available time interval for each asset
  - Percentage of bad data overall and by month:
    - NaNs
    - Missing timestamp bars
    - Observations with volume=0

Example:

- `s3://alphamatic-data/notebooks/Master_single_vendor_qa.20220608-094655.html`

#### Master_cross_vendor_qa

File:

- [research_amp/cc/notebooks/Master_cross_vendor_qa.ipynb]()

Description:

- Use `ImClients` to load data for data sets (typically two vendors)
- Compare their QA stats in order to prioritize vendors
- Compute and plot vendors’ stats differences in:
  - Available universes
  - Available time intervals
  - Percentage of bad data overall and by month:
    - NaNs
    - Missing timestamp bars
    - Observations with volume=0

Example:

- `s3://alphamatic-data/notebooks/Master_cross_vendor_qa.20220608-094902.html`

# Analyze features

## Master_tradability_analysis

File:

- [/research_amp/cc/notebooks/master_tradability_analysis.ipynb]()

Description:

- Calculate various styled facts about cryptocurrencies
- Read and process OHLCV and bid / ask data in `Dataflow` style and calculate,
  visualize the following stats:
  - The distribution of (return - spread)
  - Available liquidity at the top of the book
  - Overtime quantities such as:
    - Quoted spread
    - Volatility of returns (for resampling buckets)
    - Volume profile (when there is more activity)
    - The relative spread in bps (when the cost of trading is lower / higher
      during the day)
    - The bid / ask value (when there is more intention to trade)
    - Tradability = abs(ret) / spread_bps
  - High-level stats (e.g., median relative spread, median bid / ask notional,
    volatility, volume) by coins

Example:

- `s3://alphamatic-data/notebooks/master_tradability_analysis.20220608-121710.html`

## Master_crypto_analysis

File:

- [research_amp/cc/notebooks/Master_crypto_analysis.ipynb]()

Description:

- Looks obsolete
- TODO: See if we can extract something useful. Remove it

Example:

- `s3://alphamatic-data/notebooks/Master_crypto_analysis.20220608-121842.html`

# Run backtests

## Master_DAG_debugger

Description:

- Create a DAG (all nodes are exposed and / or instantiate a pipeline from
  DAG_builder)
- Run a DAG for debugging
- Iterate

TODO(Grisha): we can re-use some code from
https://github.com/cryptokaizen/orange/blob/master/dataflow_orange/pipelines/C1/notebooks/C1b_debugging.ipynb

# Post-process backtests

## Master_model_mixer

File:

- [dataflow/model/notebooks/Master_model_mixer.ipynb]()

Description:

- Mix different models with weights assign to them
- Evaluate the portfolio for a resulting mix

Example:

- `s3://cryptokaizen-html/notebooks/Master_model_mixer.20230615-113440.html`

## Master_feature_analyzer

File:

- [dataflow/model/notebooks/Master_feature_analyzer.ipynb]()

Description:

- Need stored experiment results
- Extract feature artifacts from experiment results
- Analyze the features

Example:

- `s3://cryptokaizen-html/notebooks/Master_feature_analyzer.20230615-111727.html`

## Master_model_analyzer

File:

- [dataflow/model/notebooks/Master_model_analyzer.ipynb]()

Description:

- Need stored experiment results
- Post-process of the single model backtest flow
- Compute and analyze model performance stats

Example:

- `s3://alphamatic-data/notebooks/Master_model_analyzer.20220608-123045.html`

## Master_local_metrics

File:

- [dataflow_orange/pipelines/E1/notebooks/E1_debugging.ipynb]()

Description:

- Need an experiment config and data intervals to run the model for
- Run an experiment by dag tiles and compute intermediary and final metrics at
  each step
- Break down performance of the models from different points of view (hit rate,
  trade pnl, ...)

Example:

- `s3://alphamatic-data/notebooks/E1_debugging.20220608-123223.html`

## Master_research_backtest_analyzer

File:

- [dataflow/model/notebooks/Master_research_backtest_analyzer.ipynb]()

Description:

- Need stored experiment results
- Load a tiled backtest and performs some analysis on the PnL and other
  performance metrics

Example:

- `s3://cryptokaizen-html/notebooks/Master_research_backtest_analyzer.20230615-112654.html`

## Master_forecast_processor_reader

File:

- [oms/notebooks/Master_forecast_processor_reader.ipynb]()

Description:

- Connect to a live Portfolio
- TODO(gp): TODO(Danya): the goal is worthy but the notebook is super-obsolete
  (P1)

Example:

- `s3://alphamatic-data/notebooks/Master_forecast_processor_reader.20220608-123538.html`

## Master_machine_learning

File:

- [research_amp/cc/notebooks/Master_machine_learning.ipynb]()

Description:

- Organized according to
  [How to organize the ML pipeline into a notebook](https://docs.google.com/document/d/16M-ABJv9dFueGwwcNZk3r4qDODrOD5ffTQMfD8-ebSM/edit#heading=h.ur9d14q7t3eu)
- Read price data (e.g., OHLCV and bid / ask data) or data from a backtest run
- One can build / debug / test quickly DAG pipelines
  - Compute features, Pre-process variables, ML calculation and evaluation, etc.
- Iterate computing metrics like Master_local_metrics.ipynb \

Next steps:

- Convert the pipeline into a real DAG and run a long backtest
- Save data to process with Master_local_metrics.ipynb

Example:

- `s3://alphamatic-data/notebooks/Master_machine_learning.20220608-123756.html`

## Master_A_B_backtest_analyzer

File:

- TODO

Description:

- Read the output of two backtests
- Compare the models (using local metrics or end-to-end metrics)

## Master_save_pnl_and_trades

File:

- [dataflow/model/notebooks/Master_save_pnl_and_trades.ipynb]()

Description:

- Load results of a historical simulation
- Compute research portfolio
- Save trades and pnl to a file
- Perform prices and pnl cross-checks

Example:

- `s3://cryptokaizen-html/Master_save_pnl_and_trades.20230622-144240.html`

# Live trading

## Master_portfolio_vs_research_stats

File:

- [oms/notebooks/Master_portfolio_vs_research_stats.ipynb]()

Description:

- Compare a simulation to a real-time execution

Example:

- `s3://alphamatic-data/notebooks/Master_portfolio_vs_research_stats.20220608-123909.html`

## Master_system_run_debugger

File:

- [oms/notebooks/Master_system_run_debugger.ipynb]()

Description:

- Load prod system run output for debugging purposes.
- Load DAG output for a specific node / bar timestamp to check the data.

Example:

- `s3://cryptokaizen-html/notebooks/Master_system_run_debugger.20230713-102611.html`

# Execution analysis

## Master_execution_analysis

File:

- [oms/notebooks/Master_execution_analysis.ipynb]()

Description:

- Analyze the results of trading execution

Example:

- `s3://alphamatic-data/notebooks/Master_execution_analysis.20230511-011718.html`

## Master_broker_debugging

File:

- [oms/notebooks/Master_broker_debugging.ipynb]()

# Multi-day system reconciliation

File:

- [oms/notebooks/Master_multiday_system_reconciliation.ipynb]()

Description:

- Stitch together portfolios for multiple daily prod system runs and plots the
  resulting PnL curves

Example:

- `s3://cryptokaizen-html/notebooks/Master_multiday_system_reconciliation.20230726-130023.html`

# Backtesting

## Per-asset backtesting

#### Basic concepts

It allows to simulate a single asset over the entire period of time

- Conceptually it's like a tiled backtesting where the tile spans the entire
  period of time

Pros:

- Simple set-up to use
- It can be extended to multiple instruments by using multi-index nodes
- It is equivalent to multiple-asset backtesting when each model is univariate

Cons:

- Requires to keep in memory the entire amount of data for the whole period of
  time and for all the needed assets
- Different from how the real-time system works, since the real-time system
  works like in tiles lasting one "period" of time
- Easy to future snooping since future data is available
