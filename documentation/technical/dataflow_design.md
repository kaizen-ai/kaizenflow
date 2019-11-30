<!--ts-->
<!--te-->

# Building a DAG

## Invariants

-   A DAG is completely represented by:
    -   a `config` object
    -   a `dag_builder` function

## Config

-   The config:
    -   Is hierarchical with one subconfig per DAG node
    -   Can specify if a block is present or not in a DAG
    -   Should only include DAG node configuration parameters, and not
        information about DAG connectivity, which is specified in the DAG
        builder
-   Template configs:
    -   Are incomplete configs, with some "mandatory" parameters unspecified but
        identified, e.g., with "_DUMMY_"
    -   Have reasonable defaults for specified parameters
        -   This facilitates config extension (e.g., if we add additional
            parameters / flexibility in the future, then we should not have to
            regenerate old configs)
    -   Leave dummy parameters for frequently-varying fields, such as `ticker`
    -   Should be completable and be completed before use
    -   Should be associated with a DAG builder

## Config builders

-   Configs are built through functions that can complete a "template" config
    with some parameters passed from the command line
-   E.g.,
    ```python
    def get_kibot_returns_config(symbol: str) -> cfg.Config:
        """
        A template configuration for `get_kibot_returns_dag`.
        """
        ...
    ```
-   One config builder per config to be built (no reuse of builders)
    -   For this reason we put `nid_prefix` in the Builder constructor
    -   `nid_prefix` acts as a namespace to avoid `nid` collisions

## DAG builder methods

-   DAG builder methods accept a config and return a DAG
-   E.g.,

    ```python
    def get_kibot_returns_dag(config: cfg.Config, dag: dtf.DAG) -> dtf.DAG:
        """
        Build a DAG (which in this case is a linear pipeline) for loading Kibot
        data and generating processed returns.

        The stages are:
          - read Kibot price data
          - compute returns
          - resample returns (optional)
          - zscore returns (optional)
          - filter returns by ATH (optional)

        - `config` must reference required stages and conform to specific node
           interfaces
        """
    ```

-   Some DAG builders can also add nodes to user-specified or inferred nodes
    (e.g., a unique sink node) of an existing DAG. Thus builders allow one to
    build a complex DAG by adding in multiple steps subgraphs of nodes.

-   The DAG and config builder functions should be paired through a concrete
    `DagBuilder` class.

-   DAG builders give meaningful `nid` names to their nodes. Collisions in
    graphs build from multiple builders are avoided by the user through the
    judicious use of namespace-like nid prefixes.

## DAG and Nodes

-   The DAG structure does not know about what data is exchanged between nodes.
    Structural assumptions, e.g., column names, can and should be expressed
    through the config
    -   `dataflow_core.py` does not impose any constraints on the type of data
        that can be exchanged
    -   In practice (e.g., all concrete classes for nodes in `dataflow.py`), we
        assume that `pd.DataFrame`s are propagated
    -   The DAG `node`s are wrappers around pandas dataframes
        -   E.g., if a node receives a column multi-index dataframe (e.g., with
            multiple instruments and multiple features per instruments), the
            node (should, assuming a well-defined DAG and config) knows how to
            melt and pivot columns

## Keeping `config` and `dag_builder` in sync

-   `config` asserts if a `dag_builder` tries to access a hierarchical parameter
    that doesn't exist and reports a meaningful error of what the problem is
-   `config` tracks what parameters are accessed by `dag_builder` function
    -   a method `sanity_check` is called after the DAG is completely built and
        reports a warning for all the parameters that were not used
    -   This is mostly for a sanity check and debugging, so we don't assert

# DAG behavior

## Invariants

-   Nodes of the DAG propagate dataframes

    -   Dataframes can be column multi-index to represent higher dimensionality
        datasets (e.g., multiple instruments, multiple features for each
        instrument)
    -   The index of each dataframe is always composed of `datatime.datetime`
        -   For performance reasons, we prefer to use a single time zone (e.g.,
            ET) in the name of the columns rather than using `datetimes` with
            `tzinfo`

-   We assume that dataframes are aligned in terms of time scale
    -   I.e., the DAG has nodes that explicitly merge / align dataframes
    -   When data sources have different time resolutions, typically we perform
        outer merges either leaving nans or filling with forward fills

## Nodes

-   We strive to write functions (e.g., from `signal_processing.py`) that:

    -   can be wrapped in `Node`s
    -   operate on `pd.Series` and can be easily applied to `pd.DataFrame`
        columns when needed using `apply_to_df` decorator, or operate on
        `pd.DataFrame` directly
    -   return information about the performed operation, so that we can store
        this information in the `Node` info
    -   e.g., a skeleton of the function (e.g., `process_outliers`)

        ```python
        def ...(
            srs: pd.Series,
            ...
            stats: Optional[dict] = None,
        ) -> pd.Series:
            """
            :param srs: pd.Series to process
            ...
            :param stats: empty dict-like object that this function will populate with
                statistics about the performed operation

            :return: transformed series with ...
                The operation is not in place.
            """
            # Check parameters.
            dbg.dassert_isinstance(srs, pd.Series)
            srs = srs.copy()
            ...
            # Compute stats.
            if stats is not None:
                dbg.dassert_isinstance(stats, dict)
                # Dictionary should be empty.
                dbg.dassert(not stats)
                stats["series_name"] = srs.name
                stats["num_elems_before"] = len(srs)
                ...
            return srs
        ```

-   `ColumnTransformer` is a very flexible `Node` class that can wrap a wide
    variety of functions
    -   The function to use is passed to the `ColumnTransformer` constructor in
        the DAG builder
    -   Arguments to forward to the function are passed through
        `transformer_kwargs`
    -   Currently `ColumnTransformer` does not allow index-modifying changes (we
        may relax this constraint but continue to enforce it by default)
-   `DataframeMethodRunner` can run any `pd.DataFrame` method supported and
    forwards kwargs

# Problems

## One vs multiple graphs

-   We still don't have a final answer about this design issue

-   Pros of one graph:
    -   Everything is in one place
    -   One config for the whole graph
-   Pros of multiple graphs:
    -   Easier to parallelize
    -   Easier to manage memory
    -   Simpler to configure (maybe), e.g., templatize config
    -   One connected component (instead of a number depending upon the number
        of tickers)

## How to handle multiple features for a single instrument

-   E.g., close and volume for a single futures instrument
-   In this case we can use a dataframe with two columns `close_price` and
    `volume`
    -   Maybe the solution is to keep columns in the same dataframe either if
        they are processed in the same way (i.e., vectorized) or if the
        computing node needs to have to have both features available (like
        sklearn model)
    -   If close_price and volume are "independent", they should go in different
        branches of the graph using a "Y" split

## How to handle multiple instruments?

-   E.g., close price for multiple futures

-   We pass a dataframe with one column per instrument
-   All the transformations are then performed on a column-basis
-   We assume that the timeseries are aligned explicitly

## How to handle multiple features with multiple instruments

-   E.g., close price, high price, volume for multiple energy futures instrument
-   In this case we can use a dataframe with hierarchical columns, where the
    first dimension is the instrument, and the second dimension is the feature

## Irregular DAGs

-   E.g., if we have 10 instruments that need to use different models, we could
    build a DAG, instantiating 10 different pipelines
-   In general we try to use vectorization any time that is possible
    -   E.g., if the computation is the same, instantiate a single DAG working
        on all the 10 instruments in a single dataframe (i.e., vectorization)
    -   E.g,. if the computation is the same up to until a point, vectorize the
        common part, and then split the dataframe and use different pipelines

## Namespace vs hierarchical config

-   We recognize that sometimes we might want to call the same `dag_builder`
    function multiple times (e.g., a DAG that is built with a loop)
-   In this case it's not clear if it would be better to prefix the names of
    each node with a tag to make them unique or use hierarchical DAG
-   It seems simpler to use prefix for the tags, which is supported

## How to know what is configurable

-   By design, dataflow can loosely wrap python functions
    -   Any argument of the python function could be a configuration parameter
    -   `ColumnTransformer` is an example of an abstract node that wraps python
        functions that operate on columns independently
    -   Introspection to determine what is configurable would be best
    -   Manually specifying function parameters in config may be a reasonable
        approach for now
        -   This could be coupled with moving some responsibility to the
            `Config` class, e.g., specifying "mandatory" parameters along with
            methods to indicate which parameters are "dummies"
        -   Introspection on config should be easy (but may be hard in full
            generality on DAG building code)
    -   Having the builder completely bail out is another possible approach
-   Dataflow provides mechanisms for conceptually organizing config and mapping
    config to configurable functions. This ability is more important than making
    it easy to expose all possible configuration parameters.

## DAG extension vs copying

-   Currently DAG builders are chained by progressively extending an existing
    DAG
-   Another approach is to chain builders by constructing a new DAG from smaller
    component DAGs
    -   On the one hand, this
        -   may provide cleaner abstractions
        -   is functional
    -   On the other hand, this approach may require some customization of deep
        copies (to be determined)
        -   If we have clean configs and builders for two DAGs to be merged /
            unioned, then we could simply rebuild by chaining
        -   If one of the DAGs was built through, e.g., notebook
            experimentation, then a graph-introspective deep copy approach is
            probably needed
            -   If we perform deep copies, do we want to create new
                "uninitialized" nodes, or also copy state?
            -   The answer may depend upon the use case, e.g., notebooks vs
                production
        -   Extending DAGs node by node is in fact how they are built under the
            hood
