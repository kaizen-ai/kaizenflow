# DataFlow

<!-- toc -->

  * [Config](#config)
    + [Config representation and properties](#config-representation-and-properties)
    + [Assigning and getting Config items](#assigning-and-getting-config-items)
  * [Time semantics](#time-semantics)
  * [Different views of System components](#different-views-of-system-components)
  * [Architecture](#architecture)
    + [Component invariants](#component-invariants)
  * [DataFlow computing](#dataflow-computing)
    + [Template configs](#template-configs)
  * [DataFlow Computation Semantics](#dataflow-computation-semantics)
  * [Backtest and Experiment](#backtest-and-experiment)
    + [`ConfigBuilder`](#configbuilder)
    + [Experiment in strict and loose sense](#experiment-in-strict-and-loose-sense)
    + [`Backtest`](#backtest)
    + [`BacktestConfig`](#backtestconfig)
    + [`Experiment`](#experiment)
    + [Tiled backtest / experiment](#tiled-backtest--experiment)
    + [Tiled vs Tile](#tiled-vs-tile)
    + [Experiment (list) manager](#experiment-list-manager)
    + [`ExperimentBuilder`](#experimentbuilder)
    + [`BacktestRunner`](#backtestrunner)
    + [System](#system)
    + [SystemRunner](#systemrunner)
    + [System_TestCase](#system_testcase)
    + [Data structures](#data-structures)
    + [Major software components](#major-software-components)
- [OMS](#oms)
  * [High-level invariants](#high-level-invariants)

<!-- tocstop -->

## Config

**Config**. A `Config` is a dictionary-like object that represents parameters
used to build and configure other objects (e.g., a DAG or a System).

Each config is a hierarchical structure which consists of **Subconfigs** and
**Leaves**.

**Subconfig** is a nested object which represents a Config inside another
config. A Subconfig of a Subconfig is a Subconfig of a Config, i.e. the relation
is transitive.

**Leaf** is any object inside a Config that is used to build another object that
is not in itself a Config.

Note that a dictionary or other mapping objects are not permitted inside a
Config: each dictionary-like object should be converted to a Config and become a
Subconfig.

### Config representation and properties

A Config can be represented as a dictionary or a string.

Example of a dictionary representation:

```python
config1={
    "resample_1min": False,
    "client_config": {
        "universe": {
            "full_symbols": ["binance::ADA_USDT"],
            "universe_version": "v3",
        },
    },
    "market_data_config": {"start_ts": start_ts, "end_ts": end_ts},
}
```

In the example above:

- "resample_1min" is a leaf of the `config1`
- "client_config" is a subconfig of `config1`
- "universe" is a subconfig of "client_config"
- "market_data" config is a subconfig of "config1"
- "start_ts" and "end_ts" are leaves of "market_data_config" and `config1`

Example of a string representation:

![](./sorrentum_figs/image14.png){width="6.854779090113736in"
height="1.2303444881889765in"}

- The same values are annotated with `marked_as_used`, `writer` and `val_type`
  - `marked_as_used` determines whether the object was used to construct another
    object
  - `writer` provides a stacktrace of the piece of code which marked the object
    as used
  - `val_type` is a type of the object

### Assigning and getting Config items

- Config object has its own implementations of `__setitem__` and `__getitem__`
- A new value can be set freely like in a python Dict object
- Overwriting the value is prohibited if the value has already been used

Since Config is used to guarantee that the construction of any objects is
reproducible, there are 2 methods to `get` the value.

- `get_and_mark_as_used` is utilized when a leaf of the config is used to
  construct another object

  - When the value is used inside a constructor

  - When the value is used as a parameter in a function

Note that when selecting a subconfig the subconfig itself is not marked as used,
but its leaves are. For this reason, the user should avoid marking subconfigs as
used and instead select leaves separately.

Example of marking the subconfig as used:

```python
_ = config.get_and_mark_as_used("market_data_config")
```

![](./sorrentum_figs/image13.png){width="6.5in" height="1.1944444444444444in"}

Example of marking the leaf as used:

```python
_ = config.get_and_mark_as_used(("market_data_config", "end_ts"))
```

![](./sorrentum_figs/image10.png){width="6.5in" height="1.1388888888888888in"}

- `__getitem__` is used to select items for uses which do not affect the
  construction of other objects:

  - Logging, debugging and printing

## Time semantics

**Time semantics**. A DataFlow component can be executed or simulated accounting
for different ways to represent the passing of time. E.g., it can be simulated
in a timed or non-timed simulation, depending on how data is delivered to the
system (as it is generated or in bulk with knowledge time).

**Clock.** A function that reports the current timestamp. There are 3 versions
of clock:

1.  Static clock. A clock that remains the same during a system run.

    a. Future peeking is allowed

2.  Replayed clock. A moving clock that can be in the past or future with
    respect to a real clock

    a. Use time passing at the same pace of real-time wall-clock or

    b. Simulate time based on events, e.g., as soon as the workload
    corresponding to one timestamp is complete we move to the next timestamp,
    without waiting for the actual time to pass

    c. Future peeking is technically possible but is prohibited

3.  Real clock. The wall-clock time matches what we observe in real-life, data
    is provided to processing units as it is produced by systems.

    a. Future peeking is not possible in principle

**Knowledge time.** It is the time when data becomes available (e.g., downloaded
or computed) to a system. Each row of data is tagged with the corresponding
knowledge time. Data with knowledge time after the current clock time must not
be observable in order to avoid future peeking.

**Timed simulation**. Sometimes referred to as historical, vectorized, bulk,
batch simulation. In a timed simulation the data is provided with a clock that
reports the current timestamp. Data with knowledge time after the current
timestamp must not be observable in order to avoid future peeking.

TODO(gp): Add an example of df with forecasts explaining the timing

**Non-timed simulation**. (Sometimes referred to as event-based, reactive
simulation). Clock type is "static clock". Typically wall-clock time is a
timestamp that corresponds to the latest knowledge time (or greater) in a
dataframe. In this way all data in a dataframe is available because every row
has a knowledge time that is less than or equal to the wall-clock time. Note
that the clock is static, i.e. not moving. In a non-timed simulation, the data
is provided in a dataframe for the entire period of interest.

E.g., for a system predicting every 5 mins, all the input data are equally
spaced on a 5-min grid and indexed with knowledge time.

TODO(gp): Add an example of df with forecasts explaining the timing

```python
df["c"] = (df["a"] + df["b"]).shift(1)
```

**Real-time execution**. In real-time the clock type is "real clock".

E.g., for a system predicting every 5 mins, one forecast is delivered every 5
mins of wall-clock.

TODO(Grisha): add an example.

**Replayed simulation**. In replayed simulation, the data is provided in the
same "format" and with the same timing as it would be provided in real-time, but
the clock type is "replayed clock".

TODO(gp): Add an example of df with forecasts explaining the timing

## Different views of System components

**Different implementations of a component**. A DataFlow component is described
in terms of an interface and can have several implementations at different
levels of detail.

**Reference implementation**. A reference implementation is vendor-agnostic
implementation of a component (e.g., DataFrameImClient, DataFrameBroker)

**Vendor implementation**. A vendor implementation is a vendor-specific
implementation of a component (e.g., CcxtImClient, CcxtBroker).

**Mocked implementation**. A mocked implementation is a simulated version of a
vendor-specific component (e.g., a DataFrameCcxtBroker). A mocked component can
have the same timing semantics as the real-component (e.g., an asynchronous or
reactive implementation) or not.

## Architecture

In this section we summarize the responsibilities and the high level invariants
of each component of a `System`.

A `System` is represented in terms of a `Config`.

- Each piece of a `Config` refers to and configures a specific part of the
  `System`
- Each component should be completely configured in terms of a `Config`

### Component invariants

All data in components should be indexed by the knowledge time (i.e., when the
data became available to that component) in terms of current time.

Each component has a way to know:

- what is the current time (e.g., the real-time machine time or the simulated
  one)
- the timestamp of the current data bar it's working on

Each component

- should print its state so that one can inspect how exactly it has been
  initialized
- can be serialized and deserialized from disk
- can be mocked for simulating
- should save data in a directory as it executes to make the system observable

Models are described in terms of DAGs using the DataFlow framework

**Misc**. Models read data from historical and real-time data sets, typically
not mixing these two styles.

Raw data is typically stored in S3 bucket in the same format as it comes or in
Parquet format.

## DataFlow computing

**DataFlow framework**. DataFlow is a computing framework to implement machine
learning models that can run with minimal changes in timed, non-timed, replayed
simulation and real-time execution.

The working principle underlying DataFlow is to run a model in terms of time
slices of data so that both historical and real-time semantics can be
accommodated without changing the model.

- Some of the advantages of the DataFlow approach are:

  - Tiling to fit in memory
  - Cached computation
  - Adapt a procedural semantic to a reactive / streaming semantic
  - Handle notion of time
  - Control for future peeking
  - Suite of tools to replay and debug executions from real-time
  - Support for market data and other tabular data feeds
  - Support for knowledge time

- TODO(gp): Explain the advantages

Resampling VWAP (besides potential errors). This implies hardcoded formula in a
mix with resampling functions.

```python
vwap_approach_2 = (
        converted_data["close"] *
      converted_data["volume"]).resample(resampling_freq)
    ).mean() /
    converted_data["volume"].resample(resampling_freq).sum()
vwap_approach_2.head(3)
```

- TODO(gp): Explain this piece of code

**Dag Node**. A Dag Node is a unit of computation of a DataFlow model.

- A Dag Node has inputs, outputs, a unique node id (aka `nid`), and a state
- Typically, inputs and outputs to a Dag Node are dataframes
- A Dag node stores a value for each output and method name (e.g., methods are
  `fit`, `predict`, `save_state`, `load_state`)
- The DataFlow time slice semantics is implemented in terms of `Pandas` and
  `Sklearn` libraries

TODO(gp): Add picture.

**DataFlow model**. A DataFlow model (aka `DAG`) is a direct acyclic graph
composed of DataFlow nodes. It allows to connect, query the structure

Running a method on a Dag means running that method on all its nodes in
topological order, propagating values through the Dag nodes.

TODO(gp): Add picture.

**DagConfig**. A `Dag` can be built assembling Nodes using a function
representing the connectivity of the nodes and parameters contained in a Config
(e.g., through a call to a builder `DagBuilder.get_dag(config)`).

A DagConfig is hierarchical and contains one subconfig per Dag node. It should
only include `Dag` node configuration parameters, and not information about
`Dag` connectivity, which is specified in the `Dag` builder part.

### Template configs

- Are incomplete configs, with some "mandatory" parameters unspecified but
  clearly identified with `cconfig.DUMMY` value
- Have reasonable defaults for specified parameters
  - This facilitates config extension (e.g., if we add additional parameters /
    flexibility in the future, then we should not have to regenerate old
    configs)
- Leave dummy parameters for frequently-varying fields, such as `ticker`
- Should be completable and be completed before use
- Should be associated with a `Dag` builder

**DagBuilder**. It is an object that builds a DAG and has a
`get_config_template()` and a `get_dag()` method to keep the config and the Dag
in sync.

The client:

- calls `get_config_template()` to receive the template config
- fills / modifies the config
- uses the final config to call `get_dag(config)` and get a fully built DAG

A `DagBuilder` can be passed to other objects instead of `Dag` when the template
config is fully specified and thus the `Dag` can be constructed from it.

**DagRunner**. It is an object that allows to run a `Dag`. Different
implementations of a `DagRunner` allow to run a `Dag` on data in different ways,
e.g.,

- `FitPredictDagRunner`: implements two methods `fit` / `predict` when we want
  to learn on in-sample data and predict on out-of-sample data
- `RollingFitPredictDagRunner`: allows to fit and predict on some data using a
  rolling pattern
- `IncrementalDagRunner`: allows to run one step at a time like in real-time
- `RealTimeDagRunner`: allows to run using nodes that have a real-time semantic

## DataFlow Computation Semantics

Often raw data is available in a "long format", where the data is conditioned on
the asset (e.g., full_symbol), e.g.,

![](./sorrentum_figs/image2.png){width="5.338542213473316in"
height="1.1036406386701663in"}

DataFlow represents data through multi-index dataframes, where

- the outermost index is the "feature"

- the innermost index is the asset, e.g.,

![](./sorrentum_figs/image6.png){width="6.5in" height="1.0416666666666667in"}

The reason for this convention is that typically features are computed in a
uni-variate fashion (e.g., asset by asset), and we can get vectorization over
the assets by expressing operations in terms of the features. E.g., we can
express a feature as `(df["close", "open"].max() - df["high"]).shift(2)`.

Based on the example ./amp/dataflow/notebooks/gallery_dataflow_example.ipynb,
one can work with DataFlow at 4 levels of abstraction:

1.  Pandas long-format (non multi-index) dataframes and for-loops

    - We can do a group-by or filter by full_symbol
    - Apply the transformation on each resulting df
    - Merge the data back into a single dataframe with the long-format

2.  Pandas multiindex dataframes

    - The data is in the DataFlow native format
    - We can apply the transformation in a vectorized way
    - This approach is best for performance and with compatibility with DataFlow
      point of view
    - An alternative approach is to express multi-index transformations in terms
      of approach 1 (i.e., single asset transformations and then concatenation).
      This approach is functionally equivalent to a multi-index transformation,
      but typically slow and memory inefficient

3.  DataFlow nodes

    - A node implements a certain transformations on DataFrames according to the
      DataFlow convention and interfaces
    - Nodes operate on the multi-index representation by typically calling
      functions from level 2 above

4.  DAG
    - A series of transformations in terms of DataFlow nodes

Note that there are degrees of freedom in splitting the work between the various
layers.

E.g., code can be split in multiple functions at level 2) and then

[[http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/C1b_debugging.ipynb]{.underline}](http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/C1b_debugging.ipynb)

[[http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/Implement_RH1E.ipynb]{.underline}](http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/Implement_RH1E.ipynb)

## Backtest and Experiment

### `ConfigBuilder`

- Generates a list of fully formed (not template) configs that can be then run
- These configs can correspond to one or multiple Experiments, tiled or not (see
  below)
- Config builder accepts `BacktestConfig` as an input

### Experiment in strict and loose sense

Colloquially, we use experiment to mean different things, e.g., an experiment
can consist in:

- a backtest where we run a single Dag with a single config (e.g., when we test
  the predictive power of a single model)
- running a Dag (e.g., E8d) through multiple configs (e.g., with longer /
  shorter history) to perform an "A / B experiment"

- running completely different Dags (e.g., E1 vs E8c) to compare their
  performance

Strictly speaking, we refer to:

- The first one as a `Backtest` (which can be executed in terms of tiled configs
  or not)

- The second and the third as an `Experiment`

In practice almost any experiment we run consists of one or more backtests

### `Backtest`

- In general a "backtest" is simply code that is configured by a \*single\*
  `Config`s

  - The code contained in a backtest experiment can be anything

- Typically a backtest consists of:

  - creating a `Dag`(e.g., through a `DagBuilder`) or a `System` based on a
    config

  - running it over a period of time (e.g., through a `DagRunner`)

  - saving the result into a directory

### `BacktestConfig`

- = a config that has multiple parts configuring both what to run (e.g., a
  `Dag`) and how to run it (e.g., the universe, the period of time)

- It can correspond to multiple configs (e.g., when running a `TiledBacktest`)

- The common pattern is `<universe>-<top_n>.<trading_period>.<time_interval>`,
  e.g., `ccxt_v4-top3.5T.2019_2022` where

  - `ccxt_v4` is a specific version of universe

  - `top3` is top 3 assets, `all` means all assets in the universe

  - `5T` (5 minutes) is trading period

  - `2019-2022` is timeframe, i.e. run the model using data from 2019 to 2022

### `Experiment`

- A set of backtests to run, each of which corresponds to conceptually a single
  `Config`

- Each backtest can then be executed in a tiled fashion (e.g., by expressing it
  in terms of different configs, one per tile

In order to create the list of fully built configs, both a `Backtest` and a
`Experiment` need:

- an `BacktestBuilder` (what to run in a backtest)

- a `ConfigBuilder` (how to configure)

- dst_dir (destination dir of the entire experiment list, i.e., the one that the
  user passes to the command)

### Tiled backtest / experiment

- An experiment / backtest that is run through multiple tiles for time and
  assets

- In general this is just an implementation detail

### Tiled vs Tile

- We call "tiled" objects that are split in tiles (e.g., `TiledBacktest`), and
  "tile" objects that refer to tiling (e.g., `TileConfig`)

### Experiment (list) manager

- TODO(gp): experiment_list manager?

- Python code that runs experiments by:

  - generating a list of `Config` object to run, based on a `ConfigBuilder`
    (i.e., `run_experiment.py` and `run_notebook.py`)

### `ExperimentBuilder`

- TODO(gp): -> BacktestBuilder

- It is a function that:
  - Creates a DAG from the passed config
  - Runs the DAG
  - Saves the results in a specified directory

### `BacktestRunner`

- A test case object that:
  - runs a backtest (experiment) on a Dag and a Config
  - processes its results (e.g., check that the output is readable, extract a
    PnL curve or other statistics)

### System

- An object representing a full trading system comprising of:
  - MarketData
    - HistoricalMarketData (ImClient)
    - RealTimeMarketData
  - Dag
  - DagRunner
  - Portfolio
    - Optimizer
    - Broker

### SystemRunner

- An object that allows to build and run a System

- TODO(gp): Not sure it's needed

### System_TestCase

- TODO(gp): IMO this is a TestCase + various helpers

### Data structures

**Fill**

**Order**

### Major software components

![](./sorrentum_figs/image9.png){width="6.5in" height="1.875in"}

[[https://lucid.app/lucidchart/9ee80100-be76-42d6-ad80-531dcfee277e/edit?page=0_0&invitationId=inv_5777ae4b-d8f4-41c6-8901-cdfb93d98ca8#]{.underline}](https://lucid.app/lucidchart/9ee80100-be76-42d6-ad80-531dcfee277e/edit?page=0_0&invitationId=inv_5777ae4b-d8f4-41c6-8901-cdfb93d98ca8#)

![](./sorrentum_figs/image15.png){width="6.5in" height="4.319444444444445in"}

**ImClient**

Responsibilities:

Interactions:

Main methods:

**MarketData**

Responsibilities:

Interactions:

Main methods:

**Forecaster.** It is a DAG system that forecasts the value of the target
economic quantities (e.g.,

for each asset in the target

Responsibilities:

Interactions:

Main methods:

**process_forecasts.** Interface to execute all the predictions in a Forecast
dataframe through TargetPositionAndOrderGenerator.

This is used as an interface to simulate the effect of given forecasts under
different optimization conditions, spread, and restrictions, without running the
Forecaster.

**TargetPositionAndOrderGenerator**. Execute the forecasts by generating the
optimal target positions according to the desired criteria and by generating the
corresponding orders needed to get the system from the current to the desired
state.

TODO(gp): It also submits the orders so ForecastProcessor?

Responsibilities:

- Retrieve the current holdings from Portfolio

- Perform optimization using forecasts and current holdings to compute the
  target position

- Generate the orders needed to achieve the target positions

- Submit orders to the Broker

Interactions:

- Forecaster to receive the forecasts of returns for each asset

- Portfolio to recover the current holdings

Main methods:

- compute_target_positions_and_generate_orders(): compute the target positions
  and generate the orders needed to reach

- \_compute_target_holdings_shares(): call the Optimizer to compute the target
  holdings in shares

**Locates**.

**Restrictions**.

**Optimizer.**

Responsibilities:

Interactions:

Main methods:

**Portfolio**. A Portfolio stores information about asset and cash holdings of a
System over time.

Responsibilities:

- hold the holdings in terms of shares of each asset id and cash available

Interactions:

- MarketData to receive current prices to estimate the value of the holdings

- Accumulate statistics and

Main methods:

- mark_to_market(): estimate the value of the current holdings using the current
  market prices

- ...

**DataFramePortfolio**: an implementation of a Portfolio backed by a DataFrame.
This is used to simulate a system on an order-by-order basis. This should be
equivalent to using a DatabasePortfolio but without the complexity of querying a
DB.

**DatabasePortfolio**: an implementation of a Portfolio backed by an SQL
Database to simulate systems where the Portfolio state is held in a database.
This allows to simulate a system on an order-by-order basis.

**Broker.** A Broker is an object to place orders to the market and to receive
fills, adapting Order and Fill objects to the corresponding market-specific
objects. In practice Broker adapts the internal representation of Order and
Fills to the ones that are specific to the target market.

Responsibilities:

- Submit orders to MarketOms
- Wait to ensure that orders were properly accepted by MarketOms
- Execute complex orders (e.g., TWAP, VWAP, pegged orders) interacting with the
  target market
- Receive fill information from the target market

Interactions:

- MarketData to receive prices and other information necessary to execute orders
- MarketOms to place orders and receive fills

Main methods:

- submit_orders()
- get_fills()

**MarketOms**. MarketOms is the interface that allows to place orders and
receive back fills to the specific target market. This is provided as-is and
it's not under control of the user or of the protocol

- E.g., a specific exchange API interface

**OrderProcessor**

- TODO(gp): Maybe MockedMarketOms since that's the actual function?

**OmsDb**

**TO REORG**

From ./oms/architecture.md

Invariants and conventions

- In this doc we use the new names for concepts and use "aka" to refer to the
  old name, if needed

- We refer to:
- The as-of-date for a query as `as_of_timestamp`
- The actual time from `get_wall_clock_time()` as `wall_clock_timestamp`
- Objects need to use `get_wall_clock_time()` to get the "actual" time
- We don't want to pass `wall_clock_timestamp` because this is dangerous
- It is difficult to enforce that there is no future peeking when one object
  tells another what time it is, since there is no way for the second object to
  double check that the wall clock time is accurate

- We pass `wall_clock_timestamp` only when one "action" happens atomically but
  it is split in multiple functions that need to all share this information.
  This approach should be the exception to the rule of calling

`get_wall_clock_time()`

- It's ok to ask for a view of the world as of `as_of_timestamp`, but then the
  queried object needs to check that there is no future peeking by using
  `get_wall_clock_time()`

- Objects might need to get `event_loop`

- TODO(gp): Clean it up so that we pass event loop all the times and event loop
  has a reference to the global `get_wall_clock_time()`

- The Optimizer only thinks in terms of dollar

Implementation

process_forecasts()

- Aka `place_trades()`
- Act on the forecasts by:
- Get the state of portfolio (by getting fills from previous clock)
- Updating the portfolio holdings
- Computing the optimal positions
- Submitting the corresponding orders
- `optimize_positions()`
- Aka `optimize_and_update()`
- Calls the Optimizer
- `compute_target_positions()`
- Aka `compute_trades()`
- `submit_orders()`
- Call `Broker`
- `get_fills()`
- Call `Broker`
- For IS it is different
- `update_portfolio()`
- Call `Portfolio`
- For IS it is different
- It should not use any concrete implementation but only `Abstract\*`

Portfolio

- `get_holdings()`
- Abstract because IS, Mocked, Simulated have a different implementations
- `mark_to_market()`

- Not abstract
- -> `get_holdings()`, `PriceInterface`
- `update_state()`
- Abstract
- Use abstract but make it NotImplemented (we will get some static checks and
  some other dynamic checks)
- We are trying not to mix static typing and duck typing
- CASH_ID, `_compute_statistics()` goes in `Portolio`

Broker

- `submit_orders()`
- `get_fills()`

Simulation

DataFramePortfolio

- This is what we call `Portfolio`
- In RT we can run `DataFramePortfolio` and `ImplementedPortfolio` in parallel
  to collect real and simulated behavior
- `get_holdings()`
- Store the holdings in a df
- `update_state()`
- Update the holdings with fills -> `SimulatedBroker.get_fills()`
- To make the simulated system closer to the implemented

SimulatedBroker

- `submit_orders()`
- `get_fills()`

Implemented system

ImplementedPortfolio

- `get_holdings()`
- Check self-consistency and assumptions
- Check that no order is in flight otherwise we should assert or log an error
- Query the DB and gets you the answer
- `update_state()`
- No-op since the portfolio is updated automatically

ImplementedBroker

- `submit_orders()`
- Save files in the proper location
- Wait for orders to be accepted
- `get_fills`
- No-op since the portfolio is updated automatically

Mocked system

- Our implementation of the implemented system where we replace DB with a mock
- The mocked DB should be as similar as possible to the implemented DB

DatabasePortfolio

- `get_holdings()`
- Same behavior of `ImplementedPortfolio` but using `OmsDb`

DatabaseBroker

- `submit_orders()`
- Same behavior of `ImplementedBroker` but using `OmsDb`

OmsDb

- `submitted_orders` table (mocks S3)
- Contain the submitted orders
- `accepted_orders` table
- `current_position` table

OrderProcessor

- Monitor `OmsDb.submitted_orders`
- Update `OmsDb.accepted_orders`
- Update `OmsDb.current_position` using `Fill` and updating the `Portfolio`

# OMS

## High-level invariants

- The notion of parent vs child orders is only on our side, CCXT only
  understands orders
- We track data (e.g., orders, fills) into parallel OMS vs CCXT data structures
  - OMS vs CCXT orders
  - OMS vs CCXT fills
  - CCXT trades vs fills
- `Portfolio` only cares about parent orders

  - How orders are executed is an implementation detail
  - Thus, we need to fold all child fills into an equivalent parent fill

- The data

- `ccxt_order`

  - Every time we submit an order to CCXT (parent or child) we get back a
    `ccxt_order` object (aka `ccxt_order_response`)
  - It's mainly a confirmation of the order
  - The format is https://docs.ccxt.com/#/?id=order-structure
  - The most important info is the callback CCXT ID of the order (this is the
    only way to do it)

- `ccxt_trade`
  - For each order (parent or child), we get back from CCXT 0 or more
    `ccxt_trade`, each representing a partial fill (e.g., price, number of
    shares, fees)
  - E.g.,
    - If we walk the book, we obviously get multiple `ccxt_trade`
    - If we match multiple trades even at the same price level, we might get
      different `ccxt_trade`
- `ccxt_fill`
  - When an order closes, we can ask CCXT to summarize the results of that order
    in terms of the composing trades
  - We can't use `ccxt_fill` to create an `oms_fill` because it doesn't contain
    enough info about prices and fees
    - We get some information about quantity, but we don't get fees and other
      info (e.g., prices)
  - We save this info to cross-check `ccxt_fill` vs `ccxt_trade`
- `oms_fill`
  - It represents the fill of a parent order, since outside the execution system
    (e.g., in `Portfolio`) we don't care about tracking individual fills
  - For a parent order we need to convert multiple `ccxt_trades` into a single
    `oms_fill`
  - TODO(gp): Unclear
  - Get all the trades combined to the parent order to get a single OMS fill
    - We use this v1
  - Another way is to query the state of an order
    - We use this in v2 prototype, but it's not sure that it's the final
      approach
