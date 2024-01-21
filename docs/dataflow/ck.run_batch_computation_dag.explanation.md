

<!-- toc -->

- [Overview](#overview)
- [DagBuilder](#dagbuilder)
  * [DAG config](#dag-config)
- [DAG](#dag)
  * [Source node](#source-node)
  * [DAG node](#dag-node)
- [DagRunner](#dagrunner)

<!-- tocstop -->

# Overview

The flow consists of the following stages:

- Define a model's configuration using a `DagBuilder` object which contains
  information about:
  - What classes are the `Nodes`
  - In which order to execute the `Nodes`
  - What is the configuration for each `Node`
- Build a `Dag` given the model configuration from a `DagBuilder`
- Connect a `DataSource` to the `Dag` to specify which data to use an input
- Run the `Dag` in the batch mode using `FitPredictDagRunner`

# DagBuilder

- `DagBuilder` is an object that builds a `Dag`
  - `get_config_template()` defines the configuration for each `Node`
  - `get_dag()` defines what classes `Nodes` are and how they are connected
- A user:
  - Calls `get_config_template()` to receive the template `Config`
  - Fills / modifies the `Config`
  - Uses the final config to call `get_dag(config)` and get a fully built `Dag`

## `Dag` config

- A `Dag` can be built assembling `Nodes` using a function representing the
  connectivity of the nodes and parameters contained in a Config (e.g., through
  a call to a builder `DagBuilder.get_dag(config))`
- A `Dag` config is hierarchical and contains one subconfig per `Dag` `Node`. It
  should only include `Dag` `Node` configuration parameters, and not information
  about `Dag` connectivity, which is specified in the `get_dag()` part
- E.g.,
  ```markdown
  sklearn: 
    in_col_groups: [('ret_0',), ('x_1',), ('x_2',)]
    out_col_group: ()
    x_vars: ['x_1', 'x_2']
    y_vars: ['ret_0']
    steps_ahead: 1
    model_kwargs: 
      fit_intercept: False
    nan_mode: drop
  residualize: 
    in_col_groups: [('ret_0.shift_-1',), ('ret_0.shift_-1_hat',)]
    out_col_group: ()
    transformer_kwargs: 
      term1_col: ret_0.shift_-1
      term2_col: ret_0.shift_-1_hat
      out_col: residual.shift_-1
      operation: sub
  compute_rolling_norm: 
    in_col_group: ('residual.shift_-1',)
    out_col_group: ('smoothed_squared_residual.shift_-1',)
    transformer_kwargs: 
      tau: 10
  ```

# `Dag`

- A DataFlow model (aka `Dag`) is a direct acyclic graph composed of DataFlow
  `Nodes`
- A `Dag` allows to connect, query the structure, execute `Nodes`, etc.
- Running a method on a `Dag` means running that method on all its nodes in
  topological order, propagating values through the `Dag` `Nodes`

## Source node

- A `DataSource` node defines the input data for a `Dag`
- A DataFrame may be wrapped in a DataFlow `DataSource` node, e.g.,
  `DfDataSource`
  - Accepts data as a DataFrame passed through the constructor and outputs the
    data
- This is the `Dag` entry point

## `Dag` `Node`

- A `Dag` `Node` is a unit of computation of a DataFlow model
- A `Dag` `Node` has inputs, outputs, a unique node id (aka nid), and a state
  - The output of a DataFlow `Node` is a dictionary of DataFrames
  - In most cases, a `Node` has a single output, and "df_out" is the default
    dictionary key
- A `Dag` `Node` implements specific transformations on inputs to generate
  the outputs
- A `Dag` `Node` stores a value for each output and method name
  - E.g., some methods are `fit()`, `predict()`, `save_state()`, `load_state()`
  - Two of the most important methods on a dataflow are `fit()` and `predict()`.
    These methods only differ if the node learns state, e.g., fits a smoothing
    parameter or learns regression coefficients. In those cases, setting
    appropriate `fit()` and `predict()` time intervals is important

# `DagRunner`

- It is an object that allows to run a `Dag`
- Different implementations of a `DagRunner` allow to run a `Dag` on data in
  different ways, e.g.,
  - `FitPredictDagRunner`: implements two methods fit / predict when we want to
    learn on in-sample data and predict on out-of-sample data
  - `RollingFitPredictDagRunner`: allows to fit and predict on some data using a
    rolling pattern
  - `RealTimeDagRunner`: allows to run using nodes that have a real-time
    semantic
