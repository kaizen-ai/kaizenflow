

<!-- toc -->

- [System](#system)
  * [Code](#code)
  * [What is a System](#what-is-a-system)
  * [`System` vs `DagBuilder`](#system-vs-dagbuilder)
  * [Using a `System`](#using-a-system)
  * [Design invariants](#design-invariants)
  * [Inheritance style conventions](#inheritance-style-conventions)
  * [SystemConfig structure](#systemconfig-structure)
  * [Trading period vs forecast period](#trading-period-vs-forecast-period)
- [Reference code](#reference-code)
- [High-level invariants](#high-level-invariants)
- [Applying overrides to SystemConfig](#applying-overrides-to-systemconfig)
  * [Overriding Config format](#overriding-config-format)
  * [The problem with hardwired configuration parameters](#the-problem-with-hardwired-configuration-parameters)
- [SystemConfig representation](#systemconfig-representation)
  * [How to select config attributes](#how-to-select-config-attributes)
  * [How to handle composition](#how-to-handle-composition)
  * [How to handle inheritance](#how-to-handle-inheritance)
  * [How to include components configuration into a SystemConfig](#how-to-include-components-configuration-into-a-systemconfig)
- [A complete examples](#a-complete-examples)
- [Generating a `System` from a `SystemConfig`](#generating-a-system-from-a-systemconfig)
- [Design issues](#design-issues)
  * [How we are doing it now](#how-we-are-doing-it-now)
- [User needs](#user-needs)
- [B(a, w)](#ba-w)
- [Next steps](#next-steps)
- [Invariants](#invariants)
- [Exercises](#exercises)

<!-- tocstop -->

# System

## Code

- The code is under `dataflow/system`

## What is a System

- `System` is a class that configures and builds a machine-learning system

- A typical example of a `System` is a DAG that contains various components,
  such as:
  - A `MarketData`
  - A `DagRunner`, representing a forecast pipeline (aka `Dag`)
  - A `Portfolio`
  - A `Broker`

- The goal of a `System` class is to:
  - Create a system config describing the entire system, including the DAG
    config
  - Expose methods to build the various needed objects, e.g., `DagRunner`,
    `Portfolio`

- It is configured through a `Config` referred to as `SystemConfig`
  - The `Config` is stored inside the `System` and contains all the params and
    the objects to be built
- A `SystemConfig` describes completely a `System`

## `System` vs `DagBuilder`

- A `System` is the analogue of a `DagBuilder` for a full system, instead of a
  `Dag`
- They both have functions to:
  - Create configs (e.g., `get_template_config()` vs
    `get_system_config_template()`)
  - Create objects (e.g., `get_dag()` vs `get_dag_runner()`)

## Using a `System`

- The lifecycle of `System` is represented by the following Python code:
  ```python
  # Instantiate a System.
  system = XYZ_ForecastSystem()
  # Get the template config.
  system_config = system.get_system_config_template()
  # Apply all the changes to the `system_config` to customize the config.
  system.config[...] = ...
  ...
  # Once the system config is complete, build the system.
  dag_runner = system.dag_runner
  # Run the system.
  dag_runner.set_fit_intervals(...)
  dag_runner.fit()
  ```

## Design invariants

- A `SystemConfig` should contain all the information needed to build and run a
  `System`, in the same way a `dag_config` contains all the information to build
  a `DAG`
- It's ok to save in the config temporary information (e.g., `dag_builder`)
  - TODO(gp): We don't want to abuse this, by using the `Config` to pass
    information around
- We could add `abc.ABC` to the abstract class definition or not, instead of
  relying on inspecting the methods
  - No decision yet
- It is ok to use stricter or looser types in the interface (e.g.,
  `DatabasePortfolio` vs `Portfolio`), although we prefer the stricter types so
  that the linter doesn't get upset
- Objects have the `_object` suffix
- The parameters used to build objects have suffix `_config` and should be
  `Config`

1. A `SystemConfig` has multiple parts, conceptually one for each piece (e.g.,
   `MarketData`, `Portfolio`) of the `System` that needs to be built.

Each sub-config has all the params needed to build a component. E.g.,
`system.config["dag_config"]` contains only the parameters to build a `DAG` and
a corresponding builder shold not access any of the params outside sub-config.

A builder should accept only the relevant piece of `SystemConfig` to enforce the
invariant.

good:
```
portfolio_builder(system.config["portfolio_config"])
```

bad:
```
# This is bad because the `Portfolio` builder should not know about other components, e.g., it does not need `market_data_config`.
portfolio_builder(system.config)
```

## Inheritance style conventions

- Each class derives only from interfaces, i.e., classes that have all methods
  abstract
- We don't want to use inheritance to share code, but we want to explicitly call
  code that is shared through functions
  - Related classes need to specify each abstract method of the base class
    calling implementations of the methods explicitly, passing `self`, if needed
  - This makes the code easier to "resolve" for humans since everything is
    explicit and doesn't rely on the class hierarchy
- If only one class needs a specific function it's ok to inline the code
  directly in one of the abstract methods
  ```
  class FooBarSystem(System):

      def _get
  ```

- As soon as multiple classes need the same code we don't copy-paste or use
  inheritance, but refactor the common code into a function and call it from
  everywhere

- There are multiple interfaces, one for each "style" of `System`
  - `System` is the basic abstract interface
    - It contains the simplest possible trading system, which is composed of
      only a `DagRunner`
  - `ForecastSystem` is a system that emits forecasts using `MarketData` and a
    forecast Dag
  - `Time_ForecastSystem`

## SystemConfig structure

Note: all examples are from `Cx_ProdSystem_v1_20220727`.

- `System` parameters

General information about a `System`, e.g.,
```
system_class: Cx_ProdSystem_v1_20220727
system_log_dir: ./system_log_dir_20240118_15minutes
```

- Shared parameters

Parameters that can be consumed by multiple `System` components, e.g.,
```
event_loop_object: None
run_mode: paper_trading
trading_period: 12T
```

- `MarketData` config

All parameters required to build a `MarketData` object, e.g.,
```
market_data_config:
  universe_version: v7.4
  asset_ids: [6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986]
  days: None
  history_lookback: 0 days 00:15:00
```

- `Portfolio` config

All parameters required to build a `Portfolio` object, e.g.,
```
portfolio_config:
  order_extra_params:
  retrieve_initial_holdings_from_db: False
  mark_to_market_col: close
  pricing_method: twap.5T
  column_remap:
    bid: bid
    ask: ask
    midpoint: midpoint
    price: close
```

- `DAG` config

All parameters required to build `DAG` Nodes.
```
  compute_bar_feature:
    in_col_groups: [('close',), ('high',), ('low',)]
    out_col_group: ()
    transformer_kwargs:
      close_col: close
      high_col: high
      low_col: low
    drop_nans: True
  resample:
    in_col_groups: [('close',), ('high',), ('low',), ('open',), ('stochastic',), ('volume',)]
    out_col_group: ()
    transformer_kwargs:
      rule: 5T
      resampling_groups: [({'close': 'close'}, 'last', {}), ({'high': 'high'}, 'max', {}), ({'low': 'low'}, 'min', {}), ({'open': 'open'}, 'first', {}), ({'volume': 'volume'}, 'sum', {'min_count': 1}), ({'close': 'twap'}, 'mean', {})]
      vwap_groups: [('close', 'volume', 'vwap'), ('stochastic', 'volume', 'cmf')]
    reindex_like_input: False
    join_output_with_input: False
  compute_vol:
    in_col_groups: [('close',), ('high',), ('low',), ('open',)]
    out_col_group: ()
    transformer_kwargs:
      estimators: ['close', 'parkinson', 'garman_klass']
      open_col: open
      high_col: high
      low_col: low
      close_col: close
      apply_log: True
      take_square_root: True
  generate_feature:
    in_col_group: ('cmf',)
    out_col_group: ('feature',)
```

- `DAG` property config

\# TODO(Grisha): should it be a part of `dag_config`?

All parameters to initialize the `DAG` class.
```
  dag_property_config:
    force_free_nodes: True
    debug_mode_config:
      save_node_io: df_as_pq
      profile_execution: True
```

- `ProcessForecasts` config

\# TODO(Grisha): technically it is a `DAG Node`, however there is a separate
sub-config for it, should we move it under `dag_config`?

All parameters required to build a `ProcessForecasts` `DAG Node`, e.g.,
```
process_forecasts_node_dict:
  prediction_col: feature
  volatility_col: garman_klass_vol
  spread_col: None
  process_forecasts_dict:
    order_config:
      order_type: price@twap
      passivity_factor: None
      order_duration_in_mins: 5
    optimizer_config:
      backend: cc_pomo
      params:
        style: cross_sectional
        kwargs:
          bulk_frac_to_remove: 0.0
          target_gmv: 3000.0
    execution_mode: real_time
    log_dir: ./system_log_dir_20240118_15minutes/process_forecasts
    ath_start_time: None
    trading_start_time: None
    ath_end_time: None
    trading_end_time: None
    liquidate_at_trading_end_time: False
    share_quantization: None
  portfolio:
    <oms.broker.ccxt.ccxt_portfolio.CcxtPortfolio at 0x7f1c8e960730>
      # holdings_shares=
      Empty DataFrame
      Columns: []
      Index: []
      # holdings_notional=
      Empty DataFrame
      Columns: []
      Index: []
      # executed_trades_shares=
      Empty DataFrame
      Columns: []
      Index: []
      # executed_trades_notional=
      Empty DataFrame
      Columns: []
      Index: []
      # pnl=
      Empty DataFrame
      Columns: []
      Index: []
      # statistics=
      Empty DataFrame
      Columns: [pnl]
      Index: []
```

- `DagRunner` config

All parameters required to build a `DagRunner`, e.g.,
```
dag_runner_config:
  wake_up_timestamp: None
  rt_timeout_in_secs_or_time: 600
  bar_duration_in_secs: 300
```

- Builder functions

There is a sub-config that shows which functions are used to build each
component, e.g.,
```
object.builder_function:
  market_object: dataflow_amp.system.Cx.Cx_prod_system._get_market_data
  portfolio_object: dataflow_amp.system.Cx.Cx_prod_system._get_portfolio
  dag_object: dataflow_amp.system.Cx.Cx_prod_system._get_dag
  dag_runner_object: dataflow_amp.system.Cx.Cx_prod_system._get_dag_runner
```

- `System` components

Each `System` component is stored in a `SystemConfig`. `System` relies on
`component.__repr__()` to print a component. E.g.,
```
  market_object: RealTimeMarketData2 at 0x7f65228b6130=(_asset_id_col=asset_id <str>, _asset_ids=[6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986] <list>, _start_time_col_name=start_timestamp <str>, _end_time_col_name=end_timestamp <str>, _columns=None <NoneType>, _sleep_in_secs=1.0 <float>, _timezone=America/New_York <str>, _column_remap=None <NoneType>, _filter_data_mode=assert <str>, _max_iterations=60 <int>, _im_client=<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient object> <im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>)
  portfolio_object:
  <oms.broker.ccxt.ccxt_portfolio.CcxtPortfolio at 0x7f6522ac6700>
    # holdings_shares=
    Empty DataFrame
    Columns: []
    Index: []
    # holdings_notional=
    Empty DataFrame
    Columns: []
    Index: []
    # executed_trades_shares=
    Empty DataFrame
    Columns: []
    Index: []
    # executed_trades_notional=
    Empty DataFrame
    Columns: []
    Index: []
    # pnl=
    Empty DataFrame
    Columns: []
    Index: []
    # statistics=
    Empty DataFrame
    Columns: [pnl]
    Index: []
  dag_object: DAG at 0x7f652258b2e0=(_nx_dag=DiGraph with 11 nodes and 10 edges <networkx.classes.digraph.DiGraph>, _name=None <NoneType>, _mode=strict <str>, _save_node_io= <str>, _profile_execution=False <bool>, _dst_dir=/data/shared/ecs_tokyo/test/system_reconciliation/C11a/prod/20240118_135500.20240118_145000/system_log_dir.manual/dag/node_io <str>, force_free_nodes=False <bool>)
  dag_runner_object:
    RealTimeDagRunner at 0x7f65216a2640=(config= <core.config.config_.Config>, _column_to_tags_mapping=None <NoneType>, _result_nid=process_forecasts <str>, _execute_rt_loop_kwargs={'get_wall_clock_time': <bound method MarketData.get_wall_clock_time of <market_data.real_time_market_data.RealTimeMarketData2 at 0x7f65228b6130>:
      _asset_id_col='asset_id' <str>
      _asset_ids='[6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986]' <list>
      _start_time_col_name='start_timestamp' <str>
      _end_time_col_name='end_timestamp' <str>
      _columns='None' <NoneType>
      _sleep_in_secs='1.0' <float>
      _timezone='America/New_York' <str>
      _column_remap='None' <NoneType>
      _filter_data_mode='assert' <str>
      _max_iterations='60' <int>
      _im_client='<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient object>' <im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>>, 'bar_duration_in_secs': 720, 'rt_timeout_in_secs_or_time': 3600} <dict>, _dst_dir=None <NoneType>, _fit_at_beginning=False <bool>, _wake_up_timestamp=2024-01-18 08:54:00-05:00 <pandas._libs.tslibs.timestamps.Timestamp>, _bar_duration_in_secs=720 <int>, _set_current_bar_timestamp=True <bool>, _max_distance_in_secs=30 <int>, _events=[] <list>)
```

Scattered thoughts: Why can't DagBuilder only appear inside of `_get_dag()`?

- Can we get rid of system_config["dag_builder_object"] and its config?
  - Claim: we need info from the DagBuilder to tell MarketData how much data to
    load => if market data needs to know about the dag builder, then either we
    should pass one object to the other (e.g., method in DagBuilder to add a
    node with market data) or DagBuilder should be a core concept in System
    Maybe the key objects for a system are:
- Market data
- Dag builder
  - Dag builder should support methods for adding a market data
  - Dag builder should also have a parameter for the type of data source node
- Dag runner

## Trading period vs forecast period

Trading period is how often a `System` triggers, e.g., compute forecasts and
trade every 5 minutes

Forecast period is how forward a `DAG` predicts in terms of unit of time, e.g.,
a `DAG` predicts 2 steps ahead and forecast period is 5 minutes -> a `DAG`
predicts 10 minutes ahead.

The goal is to keep these params separated and then assigned to the same value
when needed.

There are multiple values in the `SystemConfig` that are related:

- `("process_forecasts_node_dict", "process_forecasts_dict", "order_config", "order_duration_in_mins")`
- `("dag_config", "resample", "transformer_kwargs", "rule")`
- `("portfolio_config", "pricing_method")`
- `("dag_runner_config", "bar_duration_in_secs")`

There is also a method `DagBuilder.get_trading_period()` which means that a
model is responsible for setting a trading period.

The suggestion is to have a global parameter `system.config["trading_period"]`
that controls:

- `("process_forecasts_node_dict", "process_forecasts_dict", "order_config", "order_duration_in_mins")`
- `("portfolio_config", "pricing_method")`
- `("dag_runner_config", "bar_duration_in_secs")`

Long-term there should be a separate field in the `SystemConfig` for each of
`bar_duration`, `pricing_method` and `order_duration`. A builder assigns the
values of each sub-system based on the global `system.config["trading_period"]`
unless there are overrides or explicit params defined. In other words, use
`system.config["dag_runner_config", "bar_duration_in_secs"]` if it is specified,
otherwise use `system.config["trading_period"]` to get the value for
`system.config["dag_runner_config", "bar_duration_in_secs"]`.

While there is a separate parameter
`system.config["dag_config"]["forecast_period"]` that controls the DAG
resampling frequency in case there is a resampling `Node`.

Issue \#1:

It's not possible to modify `dag_config` as it is now. The problem is that
`DagBuilder.get_dag()` validates `dag_config` by checking if every outermost key
represents a `DAG` `Node`, i.e. `dag.get_node(key)`.

The idea was to fail fast, so before `DAG` construction the code checks that all
the params for all the nodes are available.

Solution: remove the check and assert if a `SystemConfig` has missing pieces.

Issue \#2:

The role of `DagBuilder.get_trading_period()` is unclear then. Should we kill
the method at some point?

Solution: kill the method because: a) This is specific of trading while
`DagBuilder` abstract class should remain general purpose b) A model should get
`trading_period` values (if it needs it) from `SystemConfig` rather than setting
it

Issue \#3:

For the historical DAGs there is a backtest config that controls the resampling
frequency. E.g., `ccxt_v7-all.5T.2022_01_01-2023_01-01` where `5T` represents
`trading_period_str`. Should we rename `trading_period_str` to
`forecast_period_as_pd_str` everywhere?

Solution: `backtest_config` should be killed. the issue is that it makes the
assumption `trading_period = forecast_period` which is not always True.

# Reference code

dataflow/system/system.py

# High-level invariants

- `SystemConfig` needs to be built in order
- `SystemConfig` should match the topology of the system being built
- `SystemConfig` should have one piece for each system being built
- `SystemConfig` should have configs embedded in config representing the
  sub-components
- `SystemConfig` should be easy to modify (e.g., not requiring extensive changes
  in the code)
- There is no check for semantics
- Use the kwargs trick to pass all the necessary parameters
- There should be one parameter in the config for each "independent" knob (e.g.,
  the trading frequency, the resampling rule, the frequency of repricing)
  - Each component decides how to handle the value through its builder
  - Any `System` component (e.g., `DagBuilder`) should retrieve the values from
    `SystemConfig`instead of setting them

# Applying overrides to SystemConfig

The common idiom is:

1. Instantiate a `System` object

   ```python
   system = build_system()
   ```

2. Fill in the `SystemConfig` to configure the components

   ```python
   system.config["market_data_config", "universe_version] = ...
   ...
   ```

3. Apply overrides from the command line

   ```python
   config = apply_config_overrides_from_command_line(system.config, args)
   system.set_config(config)
   ```

4. Build the `System`

   ```python
   dag_runner = system.dag_runner
   ```

5. Run the `System`

   ```python
   dag_runner.predict()
   ```

It is important to understand that:

1. `System` is built in order
   - The order of construction of the object is `MarketData` -> `Portfolio` ->
     `DAG` -> `DagRunner`
   - This is because `Portfolio` contains `MarketData`, `DAG` contains
     `Portfolio` (to process forecasts) and `DagRunner` contains `DAG`
2. `System` is built when one calls `System.dag_runner`
3. One must apply overrides before calling `System.dag_runner`. At this point
   all components are already built and overriding `SystemConfig` won't change
   components configuration

## Overriding Config format

- TODO(Grisha): explain the overrides format, e.g.,
  `--set_config_value '("optimizer_config","params","style"),(str("longitudinal"))'`.

## The problem with hardwired configuration parameters

Some component builders contain hardwired configuration parameters.

This means that the components configuration is not fully reflected in the
`SystemConfig` and filling `SystemConfig` happens together with the component
construction.

For this reason, the overriding mechanism does not work for that component.

For instance:

```python
def get_MarketData_instance1():
    param1 = "XYZ"
    param2 = system.config["market_data_config", "param2"]
    param3 = "ABC"
    system.config["market_data_config", "param3"] = param3
    market_data = MarketData(param1, param2, param3)
    return market_data
```

- `param1`:
  - It is not even written to `SystemConfig` which means that one cannot infer
    its value by inspecting `SystemConfig`
  - Since it is not a part of `SystemConfig` one cannot override its value

- `param2`:
  - If overrides are applied before calling the `System` builders, the desired
    value will be written to `SystemConfig` and used for `MarketData`
    construction

- `param3`:
  - Even though it is written to `SystemConfig`, the writing happens right
    before the `MarketData` construction, so it is not possible to override the
    value

The solution is to move out the configuration from the builder.

Invariant: `System` builders should:

1. Not assign values to the `SystemConfig`
2. Extract values from `SystemConfig`
3. Build an object using the values from `SystemConfig`

# SystemConfig representation

- `SystemConfig` files

Under `system_log_dir` a `System` saves 4 files that represent `SystemConfig`:

- `system_config.input.txt`
- `system_config.input.values_as_strings.pkl`
- `system_config.output.txt`
- `system_config.output.values_as_strings.pkl`

- `input` vs `output`

An `input` file contains `System` configuration before a `System` was built. In
other words, a file contains all the input parameters required to build a
`System`.

Example:
```
dag_config:
  compute_bar_feature:
    in_col_groups: [('close',), ('high',), ('low',)]
    out_col_group: ()
    transformer_kwargs:
      close_col: close
      high_col: high
      low_col: low
    drop_nans: True
  resample:
    in_col_groups: [('close',), ('high',), ('low',), ('open',), ('stochastic',), ('volume',)]
    out_col_group: ()
    transformer_kwargs:
      rule: 5T
      resampling_groups: [({'close': 'close'}, 'last', {}), ({'high': 'high'}, 'max', {}), ({'low': 'low'}, 'min', {}), ({'open': 'open'}, 'first', {}), ({'volume': 'volume'}, 'sum', {'min_count': 1}), ({'close': 'twap'}, 'mean', {})]
      vwap_groups: [('close', 'volume', 'vwap'), ('stochastic', 'volume', 'cmf')]
    reindex_like_input: False
    join_output_with_input: False
  compute_vol:
    in_col_groups: [('close',), ('high',), ('low',), ('open',)]
    out_col_group: ()
    transformer_kwargs:
      estimators: ['close', 'parkinson', 'garman_klass']
      open_col: open
      high_col: high
      low_col: low
      close_col: close
      apply_log: True
      take_square_root: True
  generate_feature:
    in_col_group: ('cmf',)
    out_col_group: ('feature',)
dag_builder_object: nid_prefix=
dag_builder_class: C3a_DagBuilder_tmp
event_loop_object: None
run_mode: paper_trading
system_class: Cx_ProdSystem_v1_20220727
system_log_dir: ./system_log_dir_20240118_15minutes
market_data_config:
  universe_version: v7.4
  asset_ids: [6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986]
  days: None
trading_period: 5T
dag_builder_config:
  fast_prod_setup: False
dag_property_config:
  force_free_nodes: True
  debug_mode_config:
    save_node_io: df_as_pq
    profile_execution: True
dag_runner_config:
  wake_up_timestamp: None
  rt_timeout_in_secs_or_time: 600
  bar_duration_in_secs: 300
portfolio_config:
  order_extra_params:
  retrieve_initial_holdings_from_db: False
cf_config:
  strategy: C3a
  liveness: CANDIDATE
  instance_type: PROD
  trade_date: 20240118
secret_identifier_config: binance.preprod.trading.4
process_forecasts_node_dict:
  prediction_col: feature
  volatility_col: garman_klass_vol
  spread_col: None
  process_forecasts_dict:
    order_config:
      order_type: price@twap
      passivity_factor: None
      order_duration_in_mins: 5
    optimizer_config:
      backend: cc_pomo
      params:
        style: cross_sectional
        kwargs:
          bulk_frac_to_remove: 0.0
          target_gmv: 3000.0
    execution_mode: real_time
    log_dir: ./system_log_dir_20240118_15minutes/process_forecasts
```

An `output` file contains `System` configuration after a `System` was built.
Besides input configuration a file also contains `System` components (e.g.,
`MarketData`, `DagRunner`).

Example:
```
dag_config:
  compute_bar_feature:
    in_col_groups: [('close',), ('high',), ('low',)]
    out_col_group: ()
    transformer_kwargs:
      close_col: close
      high_col: high
      low_col: low
    drop_nans: True
  resample:
    in_col_groups: [('close',), ('high',), ('low',), ('open',), ('stochastic',), ('volume',)]
    out_col_group: ()
    transformer_kwargs:
      rule: 5T
      resampling_groups: [({'close': 'close'}, 'last', {}), ({'high': 'high'}, 'max', {}), ({'low': 'low'}, 'min', {}), ({'open': 'open'}, 'first', {}), ({'volume': 'volume'}, 'sum', {'min_count': 1}), ({'close': 'twap'}, 'mean', {})]
      vwap_groups: [('close', 'volume', 'vwap'), ('stochastic', 'volume', 'cmf')]
    reindex_like_input: False
    join_output_with_input: False
  compute_vol:
    in_col_groups: [('close',), ('high',), ('low',), ('open',)]
    out_col_group: ()
    transformer_kwargs:
      estimators: ['close', 'parkinson', 'garman_klass']
      open_col: open
      high_col: high
      low_col: low
      close_col: close
      apply_log: True
      take_square_root: True
  generate_feature:
    in_col_group: ('cmf',)
    out_col_group: ('feature',)
dag_builder_object: nid_prefix=
dag_builder_class: C3a_DagBuilder_tmp
event_loop_object: None
run_mode: paper_trading
system_class: Cx_ProdSystem_v1_20220727
system_log_dir: ./system_log_dir_20240118_15minutes
market_data_config:
  universe_version: v7.4
  asset_ids: [6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986]
  days: None
  history_lookback: 0 days 00:15:00
trading_period: 5T
dag_builder_config:
  fast_prod_setup: False
dag_property_config:
  force_free_nodes: True
  debug_mode_config:
    save_node_io: df_as_pq
    profile_execution: True
    dst_dir: ./system_log_dir_20240118_15minutes/dag/node_io
dag_runner_config:
  wake_up_timestamp: None
  rt_timeout_in_secs_or_time: 600
  bar_duration_in_secs: 300
portfolio_config:
  order_extra_params:
  retrieve_initial_holdings_from_db: False
  mark_to_market_col: close
  pricing_method: twap.5T
  column_remap:
    bid: bid
    ask: ask
    midpoint: midpoint
    price: close
cf_config:
  strategy: C3a
  liveness: CANDIDATE
  instance_type: PROD
  trade_date: 20240118
secret_identifier_config: binance.preprod.trading.4
process_forecasts_node_dict:
  prediction_col: feature
  volatility_col: garman_klass_vol
  spread_col: None
  process_forecasts_dict:
    order_config:
      order_type: price@twap
      passivity_factor: None
      order_duration_in_mins: 5
    optimizer_config:
      backend: cc_pomo
      params:
        style: cross_sectional
        kwargs:
          bulk_frac_to_remove: 0.0
          target_gmv: 3000.0
    execution_mode: real_time
    log_dir: ./system_log_dir_20240118_15minutes/process_forecasts
    ath_start_time: None
    trading_start_time: None
    ath_end_time: None
    trading_end_time: None
    liquidate_at_trading_end_time: False
    share_quantization: None
  portfolio:
    <oms.broker.ccxt.ccxt_portfolio.CcxtPortfolio at 0x7f1c8e960730>
      # holdings_shares=
      Empty DataFrame
      Columns: []
      Index: []
      # holdings_notional=
      Empty DataFrame
      Columns: []
      Index: []
      # executed_trades_shares=
      Empty DataFrame
      Columns: []
      Index: []
      # executed_trades_notional=
      Empty DataFrame
      Columns: []
      Index: []
      # pnl=
      Empty DataFrame
      Columns: []
      Index: []
      # statistics=
      Empty DataFrame
      Columns: [pnl]
      Index: []
market_object: RealTimeMarketData2 at 0x7f1cc98bfdc0=(_asset_id_col=asset_id <str>, _asset_ids=[6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986] <list>, _start_time_col_name=start_timestamp <str>, _end_time_col_name=end_timestamp <str>, _columns=None <NoneType>, _sleep_in_secs=1.0 <float>, _timezone=America/New_York <str>, _column_remap=None <NoneType>, _filter_data_mode=assert <str>, _max_iterations=60 <int>, _im_client=CcxtSqlRealTimeImClient at 0x7f1c8e960520=(_table_name=ccxt_ohlcv_futures <str>, _db_connection=<connection object; dsn: 'user=ck_preprod_restricted password=xxx dbname=preprod.im_data_db host=dev-im-db.cpox8ul7pzan.eu-north-1.rds.amazonaws.com port=5432', closed: 0> <psycopg2.extensions.connection>, _vendor=ccxt <str>, _universe_version=infer_from_data <str>, _resample_1min=False <bool>, _timestamp_col_name=timestamp <str>, _full_symbol_col_name=None <NoneType>, _asset_id_to_full_symbol_mapping={1528092593: 'binance::BAKE_USDT', 2601760471: 'binance::LINK_USDT', 8717633868: 'binance::AVAX_USDT', 2425308589: 'binance::STORJ_USDT', 2476706208: 'binance::CRV_USDT', 1030828978: 'binance::GMT_USDT', 3401245610: 'binance::DYDX_USDT', 1467591036: 'binance::BTC_USDT', 2683705052: 'binance::MATIC_USDT', 2540896331: 'binance::AXS_USDT', 3065029174: 'binance::DOGE_USDT', 9872743573: 'binance::NEAR_USDT', 2099673105: 'binance::RUNE_USDT', 4516629366: 'binance::SAND_USDT', 2484635488: 'binance::OGN_USDT', 5118394986: 'binance::XRP_USDT', 1464553467: 'binance::ETH_USDT', 5115052901: 'binance::CTK_USDT', 1966583502: 'binance::FTM_USDT', 1891737434: 'binance::DOT_USDT', 2384892553: 'binance::WAVES_USDT', 1776791608: 'binance::UNFI_USDT', 8968126878: 'binance::BNB_USDT', 2237530510: 'binance::SOL_USDT', 6051632686: 'binance::APE_USDT'} <dict>) <im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>)
object.builder_function:
  market_object: dataflow_amp.system.Cx.Cx_prod_system._get_market_data
  portfolio_object: dataflow_amp.system.Cx.Cx_prod_system._get_portfolio
  dag_object: dataflow_amp.system.Cx.Cx_prod_system._get_dag
  dag_runner_object: dataflow_amp.system.Cx.Cx_prod_system._get_dag_runner
portfolio_object:
  <oms.broker.ccxt.ccxt_portfolio.CcxtPortfolio at 0x7f1c8e960730>
    # holdings_shares=
    Empty DataFrame
    Columns: []
    Index: []
    # holdings_notional=
    Empty DataFrame
    Columns: []
    Index: []
    # executed_trades_shares=
    Empty DataFrame
    Columns: []
    Index: []
    # executed_trades_notional=
    Empty DataFrame
    Columns: []
    Index: []
    # pnl=
    Empty DataFrame
    Columns: []
    Index: []
    # statistics=
    Empty DataFrame
    Columns: [pnl]
    Index: []
dag_object: DAG at 0x7f1c8eb73dc0=(_nx_dag=DiGraph with 6 nodes and 5 edges <networkx.classes.digraph.DiGraph>, _name=None <NoneType>, _mode=strict <str>, _save_node_io=df_as_pq <str>, _profile_execution=True <bool>, _dst_dir=./system_log_dir_20240118_15minutes/dag/node_io <str>, force_free_nodes=True <bool>)
dag_runner_object:
  RealTimeDagRunner at 0x7f1c8d523820=(config= <core.config.config_.Config>, _column_to_tags_mapping=None <NoneType>, _result_nid=process_forecasts <str>, _execute_rt_loop_kwargs={'get_wall_clock_time': <bound method MarketData.get_wall_clock_time of <market_data.real_time_market_data.RealTimeMarketData2 at 0x7f1cc98bfdc0>:
    _asset_id_col='asset_id' <str>
    _asset_ids='[6051632686, 8717633868, 2540896331, 1528092593, 8968126878, 1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467, 1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488, 2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553, 5118394986]' <list>
    _start_time_col_name='start_timestamp' <str>
    _end_time_col_name='end_timestamp' <str>
    _columns='None' <NoneType>
    _sleep_in_secs='1.0' <float>
    _timezone='America/New_York' <str>
    _column_remap='None' <NoneType>
    _filter_data_mode='assert' <str>
    _max_iterations='60' <int>
    _im_client= <im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>
      <im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient at 0x7f1c8e960520>:
        _table_name='ccxt_ohlcv_futures' <str>
        _db_connection='<connection object; dsn: 'user=ck_preprod_restricted password=xxx dbname=preprod.im_data_db host=dev-im-db.cpox8ul7pzan.eu-north-1.rds.amazonaws.com port=5432', closed: 0>' <psycopg2.extensions.connection>
        _vendor='ccxt' <str>
        _universe_version='infer_from_data' <str>
        _resample_1min='False' <bool>
        _timestamp_col_name='timestamp' <str>
        _full_symbol_col_name='None' <NoneType>
        _asset_id_to_full_symbol_mapping= <dict>
'bar_duration_in_secs': 300, 'rt_timeout_in_secs_or_time': 600} <dict>, _dst_dir=None <NoneType>, _fit_at_beginning=False <bool>, _wake_up_timestamp=None <NoneType>, _bar_duration_in_secs=300 <int>, _set_current_bar_timestamp=True <bool>, _max_distance_in_secs=30 <int>, _events=[] <list>)
```

- `Pickle` vs `TXT`

A `TXT` file is for human consumption, a typical use-case is to check `System`
configuration after a `System` run.

A `Pickle` file is for machine consumption, a typical use-case is to compute
something using `SystemConfig` values.

`PrintableMixin` is an object that contains methods to convert an object into a
human-readable representation for both debugging and inspecting its
configuration.

Every `System` component should inherit from `PrintableMixin`.

`PrintableMixin.get_config_attributes()` is an abstract method that returns a
list of class attributes to show. Each concrete class decides which attributes
are crucial for understanding how a component is configured. A `System`
component implements its version of the abstract method
`get_config_attributes()`.

For instance:

```python
class MarketData:

   ...

   @staticmethod
   def get_config_attributes() -> List[str]:
       config_attributes = ["_asset_id_col", "_asset_ids", "_start_time_col_name"]
       return config_attributes
```

`PrintableMixin.to_config_dict()` returns a `Dict` with the following details:

- The class name
- Some class attribute names and their corresponding values

The list of attributes to show is specific to each class while building a `Dict`
of `{attribute_name: attribute_value}` pairs is common for all the classes.
`to_config_dict()` calls `get_config_attributes()` to get the list of attributes
to build a `Dict` from.

For instance:

```python
{
  "class_name": ReplayedMarketData (market_data.replayed_market_data),
  "_asset_id_col": "asset_id",
  "_asset_ids": [],
  "_start_time_col_name": "start_datetime"
}
```

The configuration of an object `to_config_dict()` can be serialized as `JSON`.

`PrintableMixin.to_config_str()`, invokes `obj_to_repr()` with
`attr_mode == "config"` that calls `get_config_attributes()` and then converts
config representation into a human-readable string format.

For instance:

```markdown
<market_data.replayed_market_data.ReplayedMarketData at 0x>:
\_asset_id_col='asset_id' <str> \_asset_ids=[] <list>
```

## How to select config attributes

There's no necessity to print all attributes, as done in, for example,
`PrintableMixin.__str__()`.

`PrintableMixin.to_config_dict()` prints only attributes considered crucial for
comprehending the configuration of a System component.

For instance, certain object attributes consist of extensive data structures,
like Pandas `DataFrames` and `Dicts`

```markdown
market_data=ReplayedMarketData (market_data.replayed_market_data):
\_asset_id_col=asset_id \_asset_ids=[101, 202] \_df= start_datetime end_datetime
0 2000-01-01 09:30:00-05:00 2000-01-01 09:31:00-05:00 0 2000-01-01
09:30:00-05:00 2000-01-01 09:31:00-05:00 1 2000-01-01 09:31:00-05:00 2000-01-01
09:32:00-05:00 1 2000-01-01 09:31:00-05:00 2000-01-01 09:32:00-05:00 2
2000-01-01 09:32:00-05:00 2000-01-01 09:33:00-05:00 .. ... ... 58 2000-01-01
10:28:00-05:00 2000-01-01 10:29:00-05:00 59 2000-01-01 10:29:00-05:00 2000-01-01
10:30:00-05:00 59 2000-01-01 10:29:00-05:00 2000-01-01 10:30:00-05:00 60
2000-01-01 10:30:00-05:00 2000-01-01 10:31:00-05:00 60 2000-01-01 10:30:00-05:00
2000-01-01 10:31:00-05:00

    [122 rows x 5 columns]
```

In this specific example, showcasing a large `DataFrame` tends to distract a
reader rather than aid in understanding how the System was configured.

When implementing `to_config_dict()`, a developer holds the freedom to determine
the crucial elements to display. This discretion allows them to highlight what
is deemed significant for comprehending the configuration, omitting elements
that might obscure the key information.

## How to handle composition

If a `System` component contains other objects (e.g., `Portfolio` contains
`Broker`, while `Broker` contains `LimitPriceComputer`) then `to_config_dict()`
prints the configuration recursively for objects that descend from
`PrintableMixin`, e.g.,

```python
{
  "class_name": Portfolio:
  "portfolio_attribute_1": ...,
  "portfolio_attribute_2": ...,
  "Broker": {
      "class_name": Broker,
      "broker_attribute_1": ...,
      "broker_attribute_2": ...,
      "LimitPriceComputer": {
          "class_name": LimitPriceComputer:
          "limit_price_computer_attribute_1": ...,
          "limit_price_computer_attribute_2": ...
      }
  }
}
```

## How to handle inheritance

For instance `ImClient` is the base class that has certain attributes (e.g.,
`_vendor`, `_universe_version`) while `SqlRealTimeImClient` is a child class of
`ImClient` and besides parent class attributes it has its own attributes (e.g.,
`_table_name`).

Every child class calls parent class `get_config_attributes()` to extend the
list of config attributes.

For instance:

```python
class SqlRealTimeImClient:

  ...

  def get_config_attributes(self) -> List[str]:
      config_attributes = super().get_config_attributes()
      child_class_attributes = ["_table_name"]
      config_attributes.extend(child_class_attributes)
      return config_attributes
```

## How to include components configuration into a SystemConfig

\# TODO(Grisha): discuss with GP.

As it is now:

To build a component `System` calls `_get_cached_value()` which stores an object
inside the config like so `self.config[key] = obj`. For instance:

```markdown
market_object: RealTimeMarketData2 at 0x7f333f3f8e50=(\_asset_id_col=asset_id
<str>, \_asset_ids=[6051632686, 8717633868, 2540896331, 1528092593, 8968126878,
1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467,
1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488,
2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553,
5118394986] <list>, \_start_time_col_name=start_timestamp <str>,
\_end_time_col_name=end_timestamp <str>, \_columns=None <NoneType>,
\_sleep_in_secs=1.0 <float>, \_timezone=America/New_York <str>,
\_column_remap=None <NoneType>, \_filter_data_mode=assert <str>,
\_max_iterations=60 <int>,
\_im_client=<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient object>
<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>)
```

We save a `SystemConfig` to a file when calling `System.dag_runner` which does
`repr(config)`. While `Config.__repr__()` calls `Config.to_string()` that
converts an object to a string like so `str(obj)`.

In other words, we still utilize `Object.__str__()` instead of
`Object.to_config_str()`.

Since this happens inside the `Config` class it is unclear who is responsible
for calling `to_config_str()` instead of `__str__()`.

Before reviewing the options below try `self.config[key] = obj.to_config_str()`
instead of `self.config[key] = obj` in `System._get_cached_value()`.

The options are:

1. In `Config` if an object is a child of `PrintableMixin` we call
   `obj.to_config_str()` instead of `str(obj)`

- We still want to use `__str__()` for regular Python data structures, e.g.,
  `int`, `List`
- However, if a `Config` key is a `System` component we should use
  `to_config_str()`
- The obvious downside is that we make a general class `Config` responsible for
  the system-specific behavior

2. Another option is to create a separate class `SystemConfig(Config)` that
   implements the desired behavior

- It looks like an overkill: create another class just to handle string
  representation

3. A more radical approach is to save all the components configuration to a
   separate file, e.g., `system_components.txt`. This implies removing the lines
   such as below from the `SystemConfig` file.

```markdown
market_object: RealTimeMarketData2 at 0x7f333f3f8e50=(\_asset_id_col=asset_id
<str>, \_asset_ids=[6051632686, 8717633868, 2540896331, 1528092593, 8968126878,
1467591036, 5115052901, 3065029174, 1891737434, 3401245610, 1464553467,
1966583502, 1030828978, 2601760471, 2683705052, 9872743573, 2484635488,
2099673105, 4516629366, 2237530510, 2425308589, 1776791608, 2384892553,
5118394986] <list>, \_start_time_col_name=start_timestamp <str>,
\_end_time_col_name=end_timestamp <str>, \_columns=None <NoneType>,
\_sleep_in_secs=1.0 <float>, \_timezone=AmericacgNew_York <str>,
\_column_remap=None <NoneType>, \_filter_data_mode=assert <str>,
\_max_iterations=60 <int>,
\_im_client=<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient object>
<im_v2.ccxt.data.client.ccxt_clients.CcxtSqlRealTimeImClient>)
```

# A complete examples

# Generating a `System` from a `SystemConfig`

# Design issues

- TODO(gp): Use the example from the white paper
- TODO(gp): Review carefully the text

- How do we specify what type of MarketData we need to build

- One approach could be to just build the Python objects one by one
  ```python
  def build_system():
      market_data = MarketData(...)
      dag_builder.connect(market_data)
      return ...
  ```
- The "config" is Python
- If you want to change a param, you need to change the code or you need to pass
  a value to the function
  ```python
  def build_system(param):
      market_data = MarketData(param)
      dag_builder.connect(market_data)
      return ...
  ```
- The "structure" is fixed but the params are customizable

- ```python
  def build_system(type_, param):
      if type_ == "type1":
        market_data = MarketData1(param)
      else:
        market_data = MarketData2(param)
      dag_builder.connect(market_data)
      return ...
  ```

- The problem with this approach is
  - You need to write code
  - It's not easy to build a signature of the System (unless each block prints
    its own "signature")
  - It's not declarative (but imperative)

## How we are doing it now

- There is a System object that represents the union of all the systems
- You inject the constructor inside certain method
- The structure is fixed by the System object
- The type of objects are specified through abstract methods
- There is a config that represents the params read by each block

- For each block there is a piece that represents the structure and a piece that
  represents the config params

# User needs

- You want to build a system but you also want to be parametrized
- Systems and configs needs to be nested because you build systems from other
  systems
- Systems and configs needs to be reusable
- We want to separate the configuration stage from the building phase
  - Config is complete and frozen and then you can build

- Building DAGs and building Systems is the same problem and we should use the
  same approach

- How do you specify the order in which objects are built and how to pick which
  constructor to call?

- The configurable piece is in the config and the structure is in code
  - DagBuilder
  - dataflow/core/dag_builder.py
  - get_config_template
    - users all the params of the system
    - tweaks them
    - get_dag() to build the object

- In Python everything is simple
  ```python
  # There is a system C that contains object A and object B. A needs to go inside B.
  a = A(x, y, z)
  b = B(a, w)
  ```
- ```python
  def get_C(x, y, z, w):
    a = A(x, y, z)
    b = B(a, w)
    return b
  ```

- The corresponding `Config` is

  ```python
  {
    # object A.
    {
      "ctor": A,
      "x": x,
      "y": y,
      "z": z
    }
    # object B.
    {
      "ctor": B,
      "param1", ? # This should be the variable a. We encode this info in the
        "structural" piece, which is Python code.
      "param2": w,
    }
  ```

- Certain params need to be in sync (e.g., x and y need to be the same)
  - This is simple to do in Python but not easy in the config
    ```python
    {
    ```

  ```python
  def get_C(x_y, z, w):
    a = A(x_y, x_y, z)
    b = B(a, w)
    return b
  ```

- We have A, B, C
  - Some params are fixed and not depend on other components
  - Some params depend on other components, in the sense that we need to call
    some method on a pre-built object (e.g., `a.get()`)
    - We match these params to None since they are unknown at config time
    - Google "field reference"
    - It's like a placeholder
  - The builder functions represents "connectivity" and some relationship
    between params
  - The variable params are specified through the interface
    ```
    # A(x, y, z) is an object that needs certain params.
    #  Params can be other objects.
    def build_A_v1(x):
      a = A(x, y0, z0)
      return a
    ```

    def build_A_v2(x, y, z): a = A(x, y, z) return a

    # B(a, w)

    def build_B_v1(a): b = B(a, w) return b

    def build_B_v2(): a = build_A_v1(x0) b = build_B_v1(a, w0) return b

    def build_B_v3(): a = build_A_v1(x0) w = a.get_w() b = B(a, w) return B

    def build_B_v4(x, y, z, w): """ This builder
    - Only specifies the structure
    - Gets all the params from the interface
    - Doesn't reuse any other builder (lazy/hacky/non-DRY approach) """ a = A(x,
      y, z) b = B(a, w) return B

    def build_B_v5(x, y, w): """ This builder
    - Only specifies the structure
    - Gets all the params from the interface
    - Doesn't reuse any other builder (lazy/hacky/non-DRY approach) """ z = z0 a
      = A(x, y, z, w) b = B(a, w) return B
    ```
    ```

- First problem:
  - How do we represent what system was built with which params?
    - Use the builder_func + the params that were passed
    - What system C did you build?
      - `git_hash, build_B_v4, x=x0, y=y0, z=z0, w=w0`
    - Go to the code and look at what has been built
    - Cons
      - It's a nightmare because now you need to navigate all the builders
    - Solution
      - Have a way to print the signature
        ```
        B = {
          A = {
            x=x0,
            y=y0,
            z=z0
          }
          w=w0
        }
        ```
      - This is a good solution, but we don't have a way to go from the
        signature to the system
      - In the past we tried to have this complete "isomorphism" where given a
        system we get a config, and given a config we get a system
        - This is not doable, since the config doesn't allow in any easy way to
          represent connectivity
        - You lose information about how certain param are related, since you
          get only the constant values
        - This is acceptable since the signature is only to verify that things
          were built correctly and store human-friendly information on how the
          system looked like
        - We don't guarantee that there is a simple way to go from the signature
          to the system

- How do we configure the Python that instantiate the stuff
  - We want to change some params from the command line
  - We want to easily tweak params in one of the builder deep inside the nesting
    of systems
  - In the same way we want to compose builders, we also compose params that we
    pass to builders
  ```
  def build_B_v5(x, y, w):
    z = z0
    a = A(x, y, z)
    b = B(a, w)
    return B
  ```
  - In this example two params are for A and 1 param is B
    - We want to keep them together, so we add hierarchy to the params
  ```
  def build_B_v5(params):
    z = z0
    a = A(*params["A"], z=z0)
    b = B(a, *params["B"])
    return B

  params = {
    "A": {
      "x": x0,
      "y": y0,
    },
    "B": {
      "w": w0,
    }
  }
  c = build_B_v5(params)
  ```
  - This nested config allows me to override params in a modular way and from
    command line
    ```
    params = {
      "A": get_A_v1(),
      "B": {
        "w": w0,
      }
    }
    ```
    - We can use `get_A_v1()` to get a set of params that is "packaged" together
      and it means a special configuration
    - This is similar to `apply`

# Next steps

- Come up with a bunch of examples of system that we want to build / configure
- Try this approach on paper to see if it works
- Look at Mock1 and write the code
- Try to build a Replayed system with this approach

# Invariants

1. The config is not the solution for everything, since structure and dependency
   between params can't be easily represented in the Config
2. Signature: it represents a System after it's built
   - All params are fixed
   - Print it recursively
   - It is human-friendly
   - You can reconstruct the System in a unique way, but not automatically
3. Structure of the system and relationship btw params is encoded in Builder
   functions
4. We allow nesting both of builders and params by composing builder functions
   and config dictionaries

Problem with current `System`

- The structure is fixed
- There is a MarketData, Portfolio, ...
- It's union of all the possible Systems

Short term: keep using the current approach to inject ReplayedXYZ

- PP with GP and Grisha
- Right now, Broker is part of Portfolio, but this is not something we are super
  happy about it

# Exercises

- Write how the config / builder approach would work for complex systems

// TODO(Grisha): add a .../system_config.tutorial.ipynb
