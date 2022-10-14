"""
Import as:

import dataflow.system.system_builder_utils as dtfssybuut
"""

import datetime
import logging
import os
from typing import Any, Callable, Coroutine, Optional, Tuple, Union

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system as dtfsyssyst
import dataflow.universe as dtfuniver
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)

# TODO(gp): -> system_builders.py

# There are different types of functions:
# 1) `apply_..._config(system, ...)`
#    - Use parameters from `system` and other inputs to populate the System config
#      with values corresponding to a certain System object
# 2) `build_..._from_System(system)`
#    - Build objects using parameters from System config

# TODO(gp): It's not clear if the `apply_...` functions should return a System or
#  just implicitly update System in place.
#  - Explicit approach of assigning System as return value
#    - Cons:
#      - adds more code `system = apply_..._config(system)`
#      - creates more code variability since we can assign the return value or not
#      - creates ambiguity since it works even if one doesn't assign it
#  - Implicit approach
#    - Pros:
#      - allows less code variation since the code is always
#        `apply_..._config(system)`
#      - requires less code
#    - Cons
#      - relies on a side effect
#  It seems that the implicit approach is the best one
#


# Maintain the functions ordered to resemble the dependency / construction order
# in a System:
# - System config
# - MarketData
# - DAG
# - Portfolio
# - Order processor
# - DAG Runner


# #############################################################################
# System config
# #############################################################################


def get_SystemConfig_template_from_DagBuilder(
    dag_builder: dtfcore.DagBuilder,
) -> cconfig.Config:
    """
    Build a System config from a DAG builder.
    """
    system_config = cconfig.Config()
    # Save the `DagBuilder` and the `DagConfig` in the config object.
    hdbg.dassert_isinstance(dag_builder, dtfcore.DagBuilder)
    #
    dag_config = dag_builder.get_config_template()
    system_config["dag_config"] = dag_config
    system_config["dag_builder_object"] = dag_builder
    # Track the name of the builder for book-keeping.
    system_config["dag_builder_class"] = dag_builder.__class__.__name__
    return system_config


def apply_backtest_config(
    system: dtfsyssyst.ForecastSystem, backtest_config: str
) -> dtfsyssyst.ForecastSystem:
    """
    Parse backtest config and fill System config for simulation.
    """
    # Parse the backtest experiment.
    (
        universe_str,
        trading_period_str,
        time_interval_str,
    ) = cconfig.parse_backtest_config(backtest_config)
    # Fill system config.
    hdbg.dassert_in(trading_period_str, ("1T", "5T", "15T"))
    system.config[
        "dag_config", "resample", "transformer_kwargs", "rule"
    ] = trading_period_str
    system.config["backtest_config", "universe_str"] = universe_str
    system.config["backtest_config", "trading_period_str"] = trading_period_str
    system.config["backtest_config", "time_interval_str"] = time_interval_str
    return system


# #############################################################################
# MarketData
# #############################################################################


def apply_MarketData_config(
    system: dtfsyssyst.ForecastSystem,
) -> dtfsyssyst.ForecastSystem:
    """
    Convert full symbol universe to asset ids and fill market data config.
    """
    im_client = build_ImClient_from_System(system)
    universe_str = system.config["backtest_config", "universe_str"]
    full_symbols = dtfuniver.get_universe(universe_str)
    asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
    #
    system.config["market_data_config", "im_client"] = im_client
    system.config["market_data_config", "asset_ids"] = asset_ids
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    return system


def build_ImClient_from_System(system: dtfsyssyst.System) -> icdc.ImClient:
    """
    Build an IM client from params in the system Config.
    """
    ctor = system.config["market_data_config", "im_client_ctor"]
    hdbg.dassert_isinstance(ctor, Callable)
    params = system.config["market_data_config", "im_client_config"]
    im_client = ctor(**params)
    hdbg.dassert_isinstance(im_client, icdc.ImClient)
    return im_client


def apply_history_lookback(
    system: dtfsyssyst.System,
    *,
    days: Optional[int] = None,
) -> dtfsyssyst.System:
    """
    Set the `history_looback` value in the system config.
    """
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    if days is None:
        days = (
            dag_builder._get_required_lookback_in_effective_days(dag_config) * 2
        )
    market_data_history_lookback = pd.Timedelta(days=days)
    system.config[
        "market_data_config", "history_lookback"
    ] = market_data_history_lookback
    return system


def get_ReplayedMarketData_from_df(
    system: dtfsyssyst.System,
) -> mdata.ReplayedMarketData:
    """
    Build an event loop MarketData with data from a dataframe stored inside the
    Config.
    """
    event_loop = system.config["event_loop_object"]
    replayed_delay_in_mins_or_timestamp = system.config[
        "market_data_config", "replayed_delay_in_mins_or_timestamp"
    ]
    data = system.config["market_data_config", "data"]
    delay_in_secs = system.config["market_data_config", "delay_in_secs"]
    market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
        event_loop,
        replayed_delay_in_mins_or_timestamp,
        data,
        delay_in_secs=delay_in_secs,
    )
    return market_data


# #############################################################################
# DAG
# #############################################################################


def get_RealTimeDag_from_System(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    # Assemble.
    dag = system.dag
    market_data = system.market_data
    market_data_history_lookback = system.config[
        "market_data_config", "history_lookback"
    ]
    process_forecasts_dict = {}
    ts_col_name = "end_datetime"
    dag = dtfsyssyst.adapt_dag_to_real_time(
        dag,
        market_data,
        market_data_history_lookback,
        process_forecasts_dict,
        ts_col_name,
    )
    _LOG.debug("dag=\n%s", dag)
    # TODO(gp): Why is this not returning anything? Is this even used?


def build_HistoricalDag_from_System(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with an historical data source for simulation.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # Prepare the DAG builder.
    dag_builder = system.config["dag_builder_object"]
    # Build the DAG.
    dag = dag_builder.get_dag(system.config["dag_config"])
    # Create HistoricalDataSource.
    stage = "read_data"
    market_data = system.market_data
    ts_col_name = "timestamp_db"
    multiindex_output = True
    col_names_to_remove = ["start_time"]  # , "timestamp_db"]
    node = dtfsysonod.HistoricalDataSource(
        stage,
        market_data,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Add the data source node.
    dag.insert_at_head(node)
    #
    system = apply_dag_property(dag, system)
    _ = system
    return dag


def apply_dag_property(
    dag: dtfcore.DAG, system: dtfsyssyst.System
) -> dtfsyssyst.System:
    """
    Apply DAG properties (e.g., `dag_builder_config`, `dag_property_config`).

    We need to pass `dag` since we can't do `system.dag` given that we
    are in the process of building it and it will cause infinite
    recursion.
    """
    dag_builder = system.config["dag_builder_object"]
    # TODO(gp): This is not a DAG property and needs to be set-up before the DAG
    #  is built. Also each piece of config should `make_read_only` the pieces that
    #  is used.
    fast_prod_setup = system.config.get(
        ["dag_builder_config", "fast_prod_setup"], False
    )
    _LOG.debug(hprint.to_str("fast_prod_setup"))
    if fast_prod_setup:
        _LOG.warning("Setting fast prod setup")
        system.config["dag_config"] = dag_builder.convert_to_fast_prod_setup(
            system.config["dag_config"]
        )
    # Set DAG properties.
    # 1) debug_mode_config
    debug_mode_config = system.config.get(
        ["dag_property_config", "debug_mode_config"], None
    )
    _LOG.debug(hprint.to_str("debug_mode_config"))
    if debug_mode_config:
        _LOG.warning("Setting debug mode")
        hdbg.dassert_not_in("dst_dir", debug_mode_config)
        # Infer the dst dir based on the `log_dir`.
        log_dir = system.config["system_log_dir"]
        # TODO(gp): the DAG should add node_io to the passed dir.
        dst_dir = os.path.join(log_dir, "dag/node_io")
        _LOG.info("Inferring dst_dir for dag as '%s'", dst_dir)
        # Update the data structures.
        debug_mode_config["dst_dir"] = dst_dir
        system.config[
            "dag_property_config", "debug_mode_config", "dst_dir"
        ] = dst_dir
        dag.set_debug_mode(**debug_mode_config)
    # 2) force_free_nodes
    force_free_nodes = system.config.get(
        ["dag_property_config", "force_free_nodes"], False
    )
    _LOG.debug(hprint.to_str("force_free_nodes"))
    if force_free_nodes:
        _LOG.warning("Setting force free nodes")
        dag.force_free_nodes = force_free_nodes
    return system


# TODO(gp): -> build_Dag_with_DataSourceNode_from_System?
def build_dag_with_data_source_node(
    system: dtfsyssyst.System,
    data_source_node: dtfcore.DataSource,
) -> dtfcore.DAG:
    """
    Create a DAG from system's DagBuilder and attach source node.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    hdbg.dassert_issubclass(data_source_node, dtfcore.DataSource)
    # Prepare the DAG builder.
    dag_builder = system.config["dag_builder_object"]
    # Build the DAG.
    dag = dag_builder.get_dag(system.config["dag_config"])
    # Add the data source node.
    dag.insert_at_head(data_source_node)
    # Build the DAG.
    system = apply_dag_property(dag, system)
    _ = system
    return dag


# TODO(gp): -> build_dag_with_RealTimeDataSource?
def add_real_time_data_source(
    system: dtfsyssyst.System,
) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    stage = "read_data"
    market_data = system.market_data
    market_data_history_lookback = system.config[
        "market_data_config", "history_lookback"
    ]
    ts_col_name = "end_datetime"
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    node = dtfsysonod.RealTimeDataSource(
        stage,
        market_data,
        market_data_history_lookback,
        ts_col_name,
        multiindex_output,
    )
    dag = build_dag_with_data_source_node(system, node)
    return dag


def add_ProcessForecastsNode(
    system: dtfsyssyst.System, dag: dtfcore.DAG
) -> dtfcore.DAG:
    """
    Append `ProcessForecastsNode` to a DAG.
    """
    _LOG.debug("")
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    stage = "process_forecasts"
    _LOG.debug("stage=%s", stage)
    node = dtfsysinod.ProcessForecastsNode(
        stage,
        **system.config["process_forecasts_node_dict"].to_dict(),
    )
    dag.append_to_tail(node)
    return dag


def apply_unit_test_log_dir(self_: Any, system: dtfsyssyst.System) -> None:
    """
    Update the `system_log_dir` to save data in the scratch space.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    system.config["system_log_dir"] = os.path.join(
        self_.get_scratch_space(), "system_log_dir"
    )


def apply_log_dir(
    self_: Any, system: dtfsyssyst.System, log_dir: Optional[str] = None
) -> None:
    """
    Update the `system_log_dir` to save data in `log_dir`.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    if log_dir is None:
        log_dir = "./system_log_dir"
    system.config["system_log_dir"] = log_dir


def apply_ProcessForecastsNode_config_for_equities(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    """
    Set the trading hours for equities.

    Equities market is open only during certain hours.
    """
    ath_start_time = datetime.time(9, 30)
    # Compute the time of the last bar before 16:00 given the trading
    # frequency.
    ath_end_time = datetime.time(16, 00)
    trading_end_time = pd.Timestamp.today().replace(
        hour=ath_end_time.hour,
        minute=ath_end_time.minute,
        second=ath_end_time.second,
        microsecond=0,
    )
    bar_duration_in_secs = system.config[
        "dag_runner_config", "bar_duration_in_secs"
    ]
    # We need to find the bar that includes 1 minute before the trading end
    # time.
    mode = "floor"
    trading_end_time = hdateti.find_bar_timestamp(
        trading_end_time - pd.Timedelta(minutes=1),
        bar_duration_in_secs,
        mode=mode,
    )
    trading_end_time = trading_end_time.time()
    #
    dict_ = {
        "ath_start_time": ath_start_time,
        "trading_start_time": ath_start_time,
        "ath_end_time": ath_end_time,
        "trading_end_time": trading_end_time,
        "liquidate_at_trading_end_time": False,
        "share_quantization": "no_quantization",
    }
    config = cconfig.Config.from_dict(dict_)
    system.config["process_forecasts_node_dict", "process_forecasts_dict"].update(
        config, update_mode="assign_if_missing"
    )
    return system


def apply_ProcessForecastsNode_config_for_crypto(
    system: dtfsyssyst.System, is_prod: bool
) -> dtfsyssyst.System:
    """
    Set the trading hours for crypto.

    For crypto we do not filter since crypto market is open 24/7.
    """
    if is_prod:
        share_quantization = "asset_specific"
    else:
        # For simplicity in the non-prod system we do not use quantization so that
        # we do not need to pass `asset_ids_to_decimals` (that we receive from 
        # broker) around.
        share_quantization = "no_quantization"
    dict_ = {
        "ath_start_time": None,
        "trading_start_time": None,
        "ath_end_time": None,
        "trading_end_time": None,
        "liquidate_at_trading_end_time": False,
        "share_quantization": share_quantization,
    }
    config = cconfig.Config.from_dict(dict_)
    system.config["process_forecasts_node_dict", "process_forecasts_dict"].update(
        config, update_mode="assign_if_missing"
    )
    return system


# #############################################################################
# Portfolio
# #############################################################################


def get_DataFramePortfolio_from_System(
    system: dtfsyssyst.System, is_prod: bool
) -> oms.Portfolio:
    """
    Build a `DataFramePortfolio` from a system config.

    :param system: the system to build a portfolio from
    :param is_prod: whether the system is going to be used for production or
        for simulation
    """
    market_data = system.market_data
    asset_ids = system.config["market_data_config", "asset_ids"]
    if is_prod:
        # Initialize `Portfolio` with parameters that are set in the example.
        portfolio = oms.get_DataFramePortfolio_example3(
            market_data=market_data, asset_ids=asset_ids
        )
    else:
        # Set event loop object for `SimulatedBroker` used in simulation.
        event_loop = system.config["event_loop_object"]
        # Initialize `Portfolio` with parameters from the system config.
        mark_to_market_col = system.config[
            "portfolio_config", "mark_to_market_col"
        ]
        pricing_method = system.config["portfolio_config", "pricing_method"]
        portfolio = oms.get_DataFramePortfolio_example1(
            event_loop,
            market_data=market_data,
            mark_to_market_col=mark_to_market_col,
            pricing_method=pricing_method,
            asset_ids=asset_ids,
        )
    # TODO(gp): We should pass the column_remap to the Portfolio builder,
    # instead of injecting it after the fact.
    portfolio.broker._column_remap = system.config[
        "portfolio_config", "column_remap"
    ]
    return portfolio


# TODO(Grisha): Generalize `get_DatabasePortfolio_from_System` and
#  `get_DataFramePortfolio_from_System`.
def get_DatabasePortfolio_from_System(
    system: dtfsyssyst.System,
) -> oms.Portfolio:
    """
    Build a `DatabasePortfolio` from a system config.
    """
    portfolio = oms.get_DatabasePortfolio_example1(
        system.config["event_loop_object"],
        system.config["db_connection_object"],
        table_name=oms.CURRENT_POSITIONS_TABLE_NAME,
        market_data=system.market_data,
        mark_to_market_col=system.config[
            "portfolio_config", "mark_to_market_col"
        ],
        pricing_method=system.config["portfolio_config", "pricing_method"],
        asset_ids=system.config["market_data_config", "asset_ids"],
    )
    # TODO(gp): We should pass the column_remap to the Portfolio builder,
    # instead of injecting it after the fact.
    portfolio.broker._column_remap = system.config[
        "portfolio_config", "column_remap"
    ]
    return portfolio


def apply_Portfolio_config(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    """
    Extend system config with parameters for `Portfolio` init.
    """
    # TODO(Grisha): @Dan do not hard-wire the values inside the function.
    system.config["portfolio_config", "mark_to_market_col"] = "close"
    system.config["portfolio_config", "pricing_method"] = "twap.5T"
    column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    system.config["portfolio_config", "column_remap"] = cconfig.Config.from_dict(
        column_remap
    )
    return system


# #############################################################################
# OrderProcessor
# #############################################################################


def get_OrderProcessor_from_System(
    system: dtfsyssyst.System,
) -> oms.OrderProcessor:
    """
    Build an OrderProcessor object from the parameters in the SystemConfig.
    """
    # TODO(gp): We use duration_in_secs to compute the termination_condition.
    termination_condition = None
    order_processor = oms.get_order_processor_example1(
        system.config["db_connection_object"],
        system.config["dag_runner_config", "bar_duration_in_secs"],
        termination_condition,
        system.config["order_processor_config", "duration_in_secs"],
        system.portfolio,
        system.config["market_data_config", "asset_id_col_name"],
        system.config[
            "order_processor_config", "max_wait_time_for_order_in_secs"
        ],
    )
    hdbg.dassert_isinstance(order_processor, oms.OrderProcessor)
    return order_processor


def get_OrderProcessorCoroutine_from_System(
    system: dtfsyssyst.System,
    order_processor: oms.OrderProcessor,
) -> Coroutine:
    """
    Build an OrderProcessor coroutine from the parameters in the SystemConfig.
    """
    _ = system
    hdbg.dassert_isinstance(order_processor, oms.OrderProcessor)
    order_processor_coroutine: Coroutine = (
        oms.get_order_processor_coroutine_example1(
            order_processor,
        )
    )
    hdbg.dassert_isinstance(order_processor_coroutine, Coroutine)
    return order_processor_coroutine


# #############################################################################
# DAG runner
# #############################################################################


def _apply_DagRunner_config(
    system: dtfsyssyst.System,
    wake_up_timestamp: Optional[datetime.date],
    bar_duration_in_secs: int,
    rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]],
    trading_period_str: str,
) -> dtfsyssyst.System:
    """
    Apply `dag_runner_config` to the system config.

    The parameters passed via `dag_runner_config` are required to
    initialize a `DagRunner` (e.g., `RealtimeDagRunner`).
    """
    if ("dag_runner_config", "rt_timeout_in_secs_or_time") in system.config:
        # Sometimes we want to override params from the test (e.g., if we want
        # to run for a shorter period than the entire day, as the prod system does).
        val = system.config[("dag_runner_config", "rt_timeout_in_secs_or_time")]
        _LOG.warning(
            "Overriding rt_timeout_in_secs_or_time=%s with value %s",
            rt_timeout_in_secs_or_time,
            val,
        )
        rt_timeout_in_secs_or_time = val
    #
    real_time_config = {
        "wake_up_timestamp": wake_up_timestamp,
        "bar_duration_in_secs": bar_duration_in_secs,
        "rt_timeout_in_secs_or_time": rt_timeout_in_secs_or_time,
        # TODO(Grisha): do we need `trading_period_str` to initialize the `RealTimeDagRunner`?
        "trading_period_str": trading_period_str,
    }
    system.config["dag_runner_config"] = cconfig.Config.from_dict(
        real_time_config
    )
    system = apply_history_lookback(system)
    return system


def _get_trading_period_str_and_bar_duration_in_secs(
    system: dtfsyssyst.System,
) -> Tuple[str, int]:
    """
    Get trading period string and sleep interval in seconds from `System`.
    """
    dag_config = system.config["dag_config"]
    dag_builder = system.config["dag_builder_object"]
    #
    trading_period_str = dag_builder.get_trading_period(dag_config)
    hdbg.dassert_in(trading_period_str, ["1T", "2T", "5T", "15T"])
    #
    bar_duration_in_secs = pd.Timedelta(trading_period_str).seconds
    return trading_period_str, bar_duration_in_secs


def apply_DagRunner_config_for_crypto(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    """
    Apply `dag_runner_config` for crypto.

    Since crypto market is open 24/7 the system can be started at any
    time and run an infinite amount of time.
    """
    (
        trading_period_str,
        bar_duration_in_secs,
    ) = _get_trading_period_str_and_bar_duration_in_secs(system)
    wake_up_timestamp = None
    rt_timeout_in_secs_or_time = None
    #
    system = _apply_DagRunner_config(
        system,
        wake_up_timestamp,
        bar_duration_in_secs,
        rt_timeout_in_secs_or_time,
        trading_period_str,
    )
    return system


def apply_DagRunner_config_for_equities(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    """
    Apply `dag_runner_config` for equities.

    For equities `wake_up_timestamp` and `rt_timeout_in_secs_or_time`
    are aligned with the start and end of a trading day for the equties
    market.
    """
    (
        trading_period_str,
        bar_duration_in_secs,
    ) = _get_trading_period_str_and_bar_duration_in_secs(system)
    # Determine when start and stop trading.
    # The system should come up around 9:37am ET and then we align to the
    # next bar.
    curr_time = system.market_data.get_wall_clock_time()
    _LOG.info("Current time=%s", curr_time)
    wake_up_timestamp = curr_time
    #
    if trading_period_str == "1T":
        # Run every 1 min.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=30, second=0, microsecond=0, nanosecond=0
        )
    elif trading_period_str == "2T":
        # Run every 2 min.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=30, second=0, microsecond=0, nanosecond=0
        )
    elif trading_period_str == "5T":
        # Run every 5 mins.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=40, second=0, microsecond=0, nanosecond=0
        )
    elif trading_period_str == "15T":
        # Run every 15 mins.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=45, second=0, microsecond=0, nanosecond=0
        )
    else:
        raise ValueError(f"Invalid trading_period_str='{trading_period_str}'")
    wake_up_timestamp = wake_up_timestamp.tz_convert("America/New_York")
    _LOG.debug(hprint.to_str("wake_up_timestamp"))
    # Get minutes for a time at which the real time loop should be terminated.
    # E.g., for trading period 2 minutes the system must shut down 2 minutes
    # before the market closes, i.e. at 15:58.
    # Ensure that bar_duration is a multiple of minutes.
    hdbg.dassert_eq(bar_duration_in_secs % 60, 0)
    rt_timeout_in_mins = 60 - int(bar_duration_in_secs / 60)
    hdbg.dassert_is_integer(rt_timeout_in_mins)
    rt_timeout_in_secs_or_time = datetime.time(15, int(rt_timeout_in_mins))
    system = _apply_DagRunner_config(
        system,
        wake_up_timestamp,
        bar_duration_in_secs,
        rt_timeout_in_secs_or_time,
        trading_period_str,
    )
    return system


# TODO(gp): @all -> get_RealtimeDagRunner or get_RealtimeDagRunner_from_system?
# TODO(gp): This seems less general than the one below.
def get_realtime_DagRunner_from_system(
    system: dtfsyssyst.System,
) -> dtfsrtdaru.RealTimeDagRunner:
    """
    Build a real-time DAG runner.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    dag = system.dag
    # TODO(gp): This should come from the config.
    bar_duration_in_secs = 5 * 60
    # Set up the event loop.
    get_wall_clock_time = system.market_data.get_wall_clock_time
    rt_timeout_in_secs_or_time = system.config["dag_runner_config"][
        "rt_timeout_in_secs_or_time"
    ]
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "bar_duration_in_secs": bar_duration_in_secs,
        "rt_timeout_in_secs_or_time": rt_timeout_in_secs_or_time,
    }
    dag_runner_kwargs = {
        "dag": dag,
        "fit_state": None,
        "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
        "dst_dir": None,
    }
    dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
    return dag_runner


def get_RealTimeDagRunner_from_System(
    system: dtfsyssyst.System,
) -> dtfsrtdaru.RealTimeDagRunner:
    """
    Build a real-time DAG runner from a system config.

    This is independent from the type of System.
    """
    # We need to make sure the DAG was built here before moving on.
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    dag = system.dag
    market_data = system.market_data
    hdbg.dassert_isinstance(market_data, mdata.MarketData)
    fit_at_beginning = system.config.get(
        ("dag_runner_config", "fit_at_beginning"), False
    )
    get_wall_clock_time = market_data.get_wall_clock_time
    # TODO(gp): This should become a builder method injecting values inside the
    #  config.
    _LOG.debug("system.config=\n%s", str(system.config))
    # TODO(Grisha): do not use default values.
    wake_up_timestamp = system.config.get(
        ("dag_runner_config", "wake_up_timestamp"), None
    )
    bar_duration_in_secs = system.config.get(
        ("dag_runner_config", "bar_duration_in_secs"), None
    )
    execute_rt_loop_config = {
        "get_wall_clock_time": get_wall_clock_time,
        "bar_duration_in_secs": system.config[
            "dag_runner_config", "bar_duration_in_secs"
        ],
        "rt_timeout_in_secs_or_time": system.config[
            "dag_runner_config", "rt_timeout_in_secs_or_time"
        ],
    }
    dag_runner_kwargs = {
        "dag": dag,
        "fit_state": None,
        "execute_rt_loop_kwargs": execute_rt_loop_config,
        "dst_dir": None,
        "fit_at_beginning": fit_at_beginning,
        "get_wall_clock_time": get_wall_clock_time,
        "wake_up_timestamp": wake_up_timestamp,
        "bar_duration_in_secs": bar_duration_in_secs,
    }
    # _LOG.debug("system=\n%s", str(system.config))
    dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
    return dag_runner
