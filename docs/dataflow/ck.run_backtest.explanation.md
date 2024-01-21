<!--ts-->
   * [Overview](#overview)
   * [Basic concepts](#basic-concepts)
      * [Backtest](#backtest)
      * [Experiment in strict and loose sense](#experiment-in-strict-and-loose-sense)
      * [Tiled backtest](#tiled-backtest)
      * [Per-asset backtesting](#per-asset-backtesting)
         * [By-asset vs tiled backtesting](#by-asset-vs-tiled-backtesting)
      * [Backtest config](#backtest-config)
      * [Config builder](#config-builder)
      * [Experiment builder](#experiment-builder)
      * [System](#system)
      * [Dag builder](#dag-builder)
   * [Implementation details](#implementation-details)
      * [Code layout](#code-layout)
      * [run_config_list.py](#run_config_listpy)
      * [run_config_stub.py](#run_config_stubpy)



<!--te-->

# Overview

- The backtest flow is used to run a model on past data and compute its
  performance metrics
  - KaizenFlow allows to run backtesting in both batch (aka historical,
    non-timed) with different levels of tile chunking and streaming mode (aka
    real-time, timed)

- The differences between batch mode and running a system in streaming mode are:
  - In batch mode there is no clock, i.e. all the data is available from the
    beginning and is processed in bulk, in one or more tiles
  - In streaming mode there is an advancing clock and data becomes available
    over time. This equivalent to running tiles that span a unit of time
    corresponding to the frequency of data arriving

- Use cases of backtest are:
  - Check proper functionality of a model
  - Evaluate a single model
    - E.g., compute profit-and-loss (aka pnl) a model could have made over time
  - Compare different models in terms of their predicting powers
  - Sweep over different model configuration to explore how performance change
    as function of these configurations

- A backtesting can be run in different `run_modes` to represent different
  levels of details, abstraction, and configuration
  - Research flow
    - There is no portfolio, i.e. positions are not computed, orders are not
      submitted, there is no interaction with an exchange
    - As a result transaction costs and other market-related factors are not
      reflected in the PnL
  - Simulation with DB
    - There is a portfolio,
  - Order processing
  - TODO(gp): Finish this

# Basic concepts

## Backtest

- In practice, a "backtest" is simply code that is configured by a _single_
  `Config`
  - The code contained in a backtest experiment can be anything
- Typically, a backtest consists of the following steps:
  - Create a `Dag`(e.g., through a `DagBuilder`) or a `System` based on a
    `SystemConfig`
  - Run the `Dag`/`System` over a period of time using a `DagRunner`
  - Save the results into a directory
  - Process the results to extract useful information from the simulation

## Experiment in strict and loose sense

- Colloquially, we use "experiment" to mean different things:
  1. Running a single `Dag` with a single config
     - E.g., when we test the predictive power of a single model
  2. Running a single `Dag` through multiple configs to perform an A/B
     experiment
     - E.g., run `C3a` through multiple configs with longer/shorter history
  3. Running different `Dag`s to compare their performance
  - E.g., run `C3a` and `C5b` and compare them
- Strictly speaking, we refer to:
  - The first one as a `Backtest`, since it's a single config and can be
    executed in terms of tiled configs or not
  - The second and the third as an `Experiment`
- Almost any experiment consists of one or more backtests

## Tiled backtest

- A tiled backtest allows to run a backtesting in "tiles" that cover multiple
  assets and/or different period of times
- The advantages of tiled backtesting are:
  - Allow to cope with memory issues by running simulations that don't fit in
    memory
  - Compute simulations incrementally
  - Parallelize and caching computation whenever possible

- Some set-ups for tiled backtests are described below
  - E.g., run all the assets for a single month (or day)
    - This is similar to how the real-time system works, since tiles storing
      single bars as new data are fed to the system one at the time
    - Splitting over time (and not assets) allows to compute cross-sectional
      `Dag` nodes and run optimizations
  - E.g., run a single asset for the entire period of time
    - E.g., this is a configuration often used for simulating futures, where a
      separate model is built for each asset
    - To reconcile a tiled experiment with the "simpler" baseline
  - It is possible to tile in different shapes over assets and time to perform
    different trade-offs between memory and speed

- All `Dag` nodes need to support tiling:
  - The source nodes support tiling natively when implemented in terms of a
    Parquet/DB backend
  - The computation nodes support tiling since this is the natural way of
    computing for `DataFlow`
  - The sink nodes need to write data in a tiled fashion
    - Data is written using Hive-partitions in Parquet

- Each tile is an experiment
  - An experiment is represented with a `Config` (as usual) and a `Dag`
  - Represent period to run
  - Assets to run
  - Meta info
    - Period of data to save (this is to handle the burn-in period needed by
      `Dag` `Node`s)
  - To generate the experiment, compute the config for all the tiles
    - There are helper functions
    - Execute the experiment (in parallel, serially, locally on multiple
      computers)
- We use the same mechanisms to run an experiment list as to run a tiled
  backtesting
  - In order words, a tiled backtesting is represented by multiple `Config`s
    referring to the same system but with different periods of times and
    universe

## Per-asset backtesting

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

### By-asset vs tiled backtesting

Although the by-asset backtesting is a special case of tiled backtesting, we
want to support both approaches for several reasons:

1.  We have code and unit tests using the by-asset backtesting approach that we
    don't want to port to the tiled approach

2.  Some simpler models might take advantage of a simpler backtesting flow

These are the same reasons why we decided to support the non multi-index nodes
although they are a special case of the multi-index ones.

Still we want to replace `ResultBundle` with the output format using Parquet
conventions in order to read the resulting data in the same way for both tiled
and by-asset backtesting

- Then we can have a unified way to compute stats / analyze

## Backtest config

- A backtest config is a string that configures a backtest and has multiple
  parts configuring both what to run (e.g., a `Dag`) and how to run it (e.g.,
  the universe, the period of time)
- It can correspond to multiple configs (e.g., when running a `TiledBacktest`)
- The common pattern is:
  `f"{universe}-{top_n}.{trading_period}.{start_date}_{end_date}`, e.g.,
  `mock1_v1-top2.5T.2000-01-01_2000-01-02` where:
  - `mock1_v1` is a specific version of universe
  - `top2` is top 2 assets, `all` means all assets in the universe
  - `5T` (5 minutes) is trading period
  - `2000-01-01` is a date to start the model run from
  - `2000-01-02` is a date to end the model run at

## Config builder

- A config builder is a function that generates a list of fully formed configs
  (i.e. `ConfigList`) that describe a DAG
- A config builder function is represented by a string that will be executed as
  Python code
  - The reason is that it is required to pass a config builder function across
    executables and a string that is converted to a function is the only viable
    option.
- A string contains the full path to a function, `BacktestConfig`, and the
  parameters required to call that function (if any).
  - E.g.,
    `dataflow_amp.system.mock1.mock1_tile_config_builders.build_Mock1_tile_config_list("mock1_v1-top2.5T.2000-01-01_2000-01-02")`

## Experiment builder

- An experiment builder is a function that is responsible for:
  - Creating a `System` object from an input `ConfigList`
  - Running a DAG
  - Saving the results
- The core difference between experiment builders is how it configures fitting
  and predicting
- There are 3 currently supported ways of fitting/predicting:
  - In-sample prediction, i.e. fit and predict using the same dataset
    - See
      [`run_in_sample_tiled_backtest()`](https://github.com/cryptokaizen/cmamp/blob/master/dataflow/backtest/master_backtest.py#L99)
      - TODO(grisha): Can we use links to the repo like
        `dataflow/backtest/master_backtest.py#L99`
  - Out-of-sample prediction, i.e. split the orinial dataset into 2 parts, fit
    using the 1st part (train set) and predict using the 2nd part (test set)
    - See
      [`run_ins_oos_tiled_backtest()`](https://github.com/cryptokaizen/cmamp/blob/master/dataflow/backtest/master_backtest.py#L136)
  - Rolling prediction, i.e. fit and predict using separate time intervals and
    roll forward the fit/predict window based on retraining frequency (e.g., 1
    week)
    - See
      [`run_rolling_tiled_backtest()`](https://github.com/cryptokaizen/cmamp/blob/master/dataflow/backtest/master_backtest.py#L187)

## System

\# TODO(Grisha): should this go to a separate `System` document?

- `System` is a class that configures and builds a machine-learning system
- A typical example of a `System` is a DAG that contains various components,
  such as:
  - A `MarketData`
  - A `DagRunner`, representing a forecast pipeline (which is often referred to
    as DAG)
  - A `Portfolio`
  - A `Broker`
- The goal of a `System` class is to:
  - Create a system config describing the entire system, including the DAG
    config
  - Expose methods to build the various needed objects, e.g., `DagRunner`,
    `Portfolio`
- It is configured through a `Config`, often referred to as `SystemConfig`
  - The `Config` is stored inside the `System` and contains all the params and
    the objects to be built
  - A `SystemConfig` describes completely a `System`
- A `System` is the analogue of a `DagBuilder` but for a full system, instead of
  a `Dag`
- They both have functions to:
  - Create configs (e.g., `get_template_config()` vs
    `get_system_config_template()`)
  - Create objects (e.g., `get_dag()` vs `get_dag_runner()`)

## Dag builder

- `DagBuilder` is a class for creating DAGs.
- Each DagBuilder is specific of a DAG.
- Concrete classes must specify:
  - `get_config_template()`
    - It returns a `Config` object that represents the parameters used to build
      the DAG
    - The config can depend upon variables used in class initialization
    - A config can be incomplete, e.g., `cconfig.DUMMY` is used for required
      fields that must be defined before the config can be used to initialize a
      DAG
  - Methods that allow changing a config in multiple places in self-consistent
    way (e.g., setting weights, setting frequency)
  - Methods that return properties of the DAG to be built (e.g., number of
    needed days of history)
  - `get_dag()`
    - It builds a DAG
    - Defines the DAG nodes and how they are connected to each other. The
      passed-in config object tells this function how to configure / initialize
      the various nodes
    - Once the DAG is built, we assume that the `DagBuilder` and the config that
      generated can be safely discarded

# Implementation details

\# TODO(Grisha): should this go to
`amp/docs/dataflow/ck.run_backtest.how_to_guide.md` or a reference doc since
these are implementation details?

## Code layout

- The code for backtesting is under `dataflow/backtest` (in dependency order)

- [/dataflow/backtest/dataflow_backtest_utils.py](/dataflow/backtest/dataflow_backtest_utils.py)
  - Utilities for backtest
- [/dataflow/backtest/master_backtest.py](/dataflow/backtest/master_backtest.py)
  - Different types of backtesting drivers (e.g., only in-sample, in/out sample,
    rolling)
- [/dataflow/backtest/backtest_api.py](/dataflow/backtest/backtest_api.py)
  - The entry points for running backtest of `System` with different configs
- [/dataflow/backtest/backtest_test_case.py](/dataflow/backtest/backtest_test_case.py)
  - TestCase to test a backtest run for a `Dag` and check its result
- [/dataflow/backtest/run_config_stub.py](/dataflow/backtest/run_config_stub.py)
  - Run a backtest for a single `Config`
- [/dataflow/backtest/run_config_list.py](/dataflow/backtest/run_config_list.py)
  - Run a backtest for a list of `Config`s

- There are multiple layers
  - Un_config_list.py`runs the backtesting of a list of`Config`s, handling
    parallelization and saving results to S3
  - `run_config_stub.py` runs a single `Config`

## `run_config_list.py`

- Run an experiment consisting of backtesting of multiple model runs based on
  the passed:
  - `experiment_builder`, which describes the type of backtest to be performed
  - `config_builder`, which describes `Dag` and `Config`s
- E.g.,

  ```bash
  docker> run_config_list.py \
      --experiment_builder "dataflow.backtest.master_backtest.run_ins_oos_backtest" \
      --config_builder "dataflow_lm.RH1E.config.build_15min_model_configs()" \
      --dst_dir experiment1 \
      --num_threads 2
  ```

- The script splits the workload in multiple command lines so that they can be
  parallelized
  - E.g., a command line like:
    ```bash
    docker> /app/amp/dataflow/backtest/run_config_list.py \
       --experiment_builder 'amp.dataflow.backtest.master_backtest.run_in_sample_tiled_backtest' \
       --config_builder 'dataflow_lime.system.E8.E8_tile_config_builders.build_E8e_tile_config_list("eg_v2_0-top1.5T.2020-01-01_2020-03-01")'
       --dst_dir /app/dataflow_lime/system/E8/test/outcomes/Test_E8e_TiledBacktest1.test_top1_JanFeb2020/tmp.scratch/run_model \
       --aws_profile am \
       --clean_dst_dir --no_confirm --num_threads serial \
       -v DEBUG
    ```
    is expanded into a series of command like:
    ```bash
    docker> /app/amp/dataflow/backtest/run_config_stub.py \
       --experiment_builder 'amp.dataflow.backtest.master_backtest.run_in_sample_tiled_backtest' \
       --config_builder 'dataflow_lime.system.E8.E8_tile_config_builders.build_E8e_tile_config_list("eg_v2_0-top1.5T.2020-01-01_2020-03-01")' \
       --config_idx 0 \
       --dst_dir /app/dataflow_lime/system/E8/test/outcomes/Test_E8e_TiledBacktest1.test_top1_JanFeb2020/tmp.scratch/run_model \
       -v INFO
    ```

## `run_config_stub.py`

- It is a Python command that allows to run a single `Config`
- It accepts:
  - An experiment builder which represents what needs to be run
    - E.g., `dataflow.backtest.master_backtest.run_ins_oos_backtest`
  - A config builder which builds a list of `Configs`
  - An index to represent which `Config` in the list to run
    - E.g.,
      `dataflow_lime.system.E8.E8_tile_config_builders. build_E8e_tile_config_list("eg_v2_0-top1.5T.2020-01-01_2020-03-01")`
  - A destination dir where to save the results

  ```bash
  docker> run_config_stub.py \
    --dst_dir nlp/test_results \
    --experiment_builder "dataflow.backtest.master_backtest.run_ins_oos_backtest" \
    --config_builder "nlp.build_configs.build_PTask1088_configs()" \
    --config_idx 0
  ```
