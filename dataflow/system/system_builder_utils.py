"""
Import as:

import dataflow.system.system_builder_utils as dtfssybuut
"""

import datetime
import logging
from typing import Callable

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system as dtfsyssyst
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# System config utils
# #############################################################################


def get_SystemConfig_template_from_DagBuilder(
    dag_builder: dtfcore.DagBuilder,
) -> cconfig.Config:
    """
    Build a system config from a DAG builder.
    """
    system_config = cconfig.Config()
    # Save the `DagBuilder` and the `DagConfig` in the config object.
    hdbg.dassert_isinstance(dag_builder, dtfcore.DagBuilder)
    dag_config = dag_builder.get_config_template()
    system_config["dag_config"] = dag_config
    system_config["dag_builder_object"] = dag_builder
    # Track the name of the builder for book-keeping.
    system_config["dag_builder_class"] = dag_builder.__class__.__name__
    return system_config


# TODO(gp): Move to dataflow/backtest
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


def apply_market_data_config(
    system: dtfsyssyst.ForecastSystem,
) -> dtfsyssyst.ForecastSystem:
    """
    Convert full symbol universe to asset ids and fill market data config.
    """
    im_client = build_im_client_from_config(system)
    universe_str = system.config["backtest_config", "universe_str"]
    full_symbols = dtfuniver.get_universe(universe_str)
    asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
    #
    system.config["market_data_config", "im_client"] = im_client
    system.config["market_data_config", "asset_ids"] = asset_ids
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    return system


def build_im_client_from_config(system: dtfsyssyst.System) -> icdc.ImClient:
    """
    Build an IM client from params in the system config.
    """
    ctor = system.config["market_data_config", "im_client_ctor"]
    hdbg.dassert_isinstance(ctor, Callable)
    params = system.config["market_data_config", "im_client_config"]
    im_client = ctor(**params)
    hdbg.dassert_isinstance(im_client, icdc.ImClient)
    return im_client


# #############################################################################
# Market data utils
# #############################################################################


def get_EventLoop_MarketData_from_df(
    system: dtfsyssyst.System,
) -> mdata.ReplayedMarketData:
    """
    Build an event loop MarketData with data from a dataframe.
    """
    event_loop = system.config["event_loop_object"]
    initial_replayed_delay = system.config[
        "market_data_config", "initial_replayed_delay"
    ]
    data = system.config["market_data_config", "data"]
    delay_in_secs = system.config["market_data_config", "delay_in_secs"]
    market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        data,
        delay_in_secs=delay_in_secs,
    )
    return market_data


# #############################################################################
# Source node instances
# #############################################################################


# #############################################################################
# DAG building utils
# #############################################################################


# TODO(gp): -> get_RealTimeDag_from_System
def adapt_dag_to_real_time_from_config(
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


# TODO(gp): -> ...from_System
def get_HistoricalDag_from_system(system: dtfsyssyst.System) -> dtfcore.DAG:
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


# #############################################################################
# SystemConfig
# #############################################################################


def apply_dag_runner_config(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    """
    Update:
    - dag_runner_config
    - market_data_config
    """
    dag_config = system.config["dag_config"]
    dag_builder = system.config["dag_builder_object"]
    trading_period_str = dag_builder.get_trading_period(dag_config)
    # Determine when start and stop trading.
    # The system should come up around 9:37am ET and then we align to the
    # next bar.
    wake_up_timestamp = system.market_data.get_wall_clock_time()
    _LOG.info("Current time=%s", wake_up_timestamp)
    wake_up_timestamp = wake_up_timestamp.tz_convert("America/New_York")
    if trading_period_str == "1T":
        # Run every 1 min.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=30, second=0, microsecond=0, nanosecond=0
        )
        sleep_interval_in_secs = 60 * 1
        # TODO(gp): Horrible confusing name.
        real_time_loop_time_out_in_secs = datetime.time(15, 59)
    elif trading_period_str == "2T":
        # Run every 2 min.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=30, second=0, microsecond=0, nanosecond=0
        )
        sleep_interval_in_secs = 60 * 2
        # TODO(gp): Horrible confusing name.
        real_time_loop_time_out_in_secs = datetime.time(15, 58)
    elif trading_period_str == "5T":
        # Run every 5 mins.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=40, second=0, microsecond=0, nanosecond=0
        )
        sleep_interval_in_secs = 60 * 5
        real_time_loop_time_out_in_secs = datetime.time(15, 55)
    elif trading_period_str == "15T":
        # Run every 15 mins.
        wake_up_timestamp = wake_up_timestamp.replace(
            hour=9, minute=45, second=0, microsecond=0, nanosecond=0
        )
        sleep_interval_in_secs = 60 * 15
        real_time_loop_time_out_in_secs = datetime.time(15, 45)
    else:
        raise ValueError(f"Invalid trading_period_str='{trading_period_str}'")
    #
    wake_up_timestamp = wake_up_timestamp.tz_convert("America/New_York")
    if ("dag_runner_config", "real_time_loop_time_out_in_secs") in system.config:
        # Sometimes we want to override params from the test (e.g., if we want
        # to run for a shorter period than the entire day, as the prod system does).
        val = system.config[
            ("dag_runner_config", "real_time_loop_time_out_in_secs")
        ]
        _LOG.warning(
            "Overriding real_time_loop_time_out_in_secs=%s with value %s",
            real_time_loop_time_out_in_secs,
            val,
        )
        real_time_loop_time_out_in_secs = val
    real_time_config = {
        "wake_up_timestamp": wake_up_timestamp,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        "real_time_loop_time_out_in_secs": real_time_loop_time_out_in_secs,
        "trading_period_str": trading_period_str,
    }
    system.config["dag_runner_config"] = cconfig.get_config_from_nested_dict(
        real_time_config
    )
    # Apply history_lookback.
    market_data_history_lookback = pd.Timedelta(
        days=dag_builder._get_required_lookback_in_effective_days(dag_config) * 2
    )
    system.config[
        "market_data_config", "history_lookback"
    ] = market_data_history_lookback
    return system


def apply_history_lookback(
    system: dtfsyssyst.System,
) -> dtfsyssyst.System:
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    market_data_history_lookback = pd.Timedelta(
        days=dag_builder._get_required_lookback_in_effective_days(dag_config) * 2
    )
    system.config[
        "market_data_config", "history_lookback"
    ] = market_data_history_lookback
    return system


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
    debug_mode_config = system.config.get(
        ["dag_property_config", "debug_mode_config"], None
    )
    _LOG.debug(hprint.to_str("debug_mode_config"))
    if debug_mode_config:
        _LOG.warning("Setting debug mode")
        dag.set_debug_mode(**debug_mode_config)
    force_free_nodes = system.config.get(
        ["dag_property_config", "force_free_nodes"], False
    )
    _LOG.debug(hprint.to_str("force_free_nodes"))
    if force_free_nodes:
        _LOG.warning("Setting force free nodes")
        dag.force_free_nodes = force_free_nodes
    return system


# TODO(gp): build_dag_with_DataSourceNode?
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


# TODO(Grisha): -> `add_RealTimeDataSource`?
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


# #############################################################################
# DAG runner instances.
# #############################################################################


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
    sleep_interval_in_secs = 5 * 60
    # Set up the event loop.
    get_wall_clock_time = system.market_data.get_wall_clock_time
    real_time_loop_time_out_in_secs = system.config["dag_runner_config"][
        "real_time_loop_time_out_in_secs"
    ]
    execute_rt_loop_kwargs = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": sleep_interval_in_secs,
        "time_out_in_secs": real_time_loop_time_out_in_secs,
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
        ("dag_runner_config", "fit_at_beginning"), True
    )
    get_wall_clock_time = market_data.get_wall_clock_time
    # TODO(gp): This should become a builder method injecting values inside the
    #  config.
    _LOG.debug("system.config=\n%s", str(system.config))
    wake_up_timestamp = system.config.get(
        ("dag_runner_config", "wake_up_timestamp"), None
    )
    grid_time_in_secs = system.config.get(
        ("dag_runner_config", "sleep_interval_in_secs"), None
    )
    execute_rt_loop_config = {
        "get_wall_clock_time": get_wall_clock_time,
        "sleep_interval_in_secs": system.config[
            "dag_runner_config", "sleep_interval_in_secs"
        ],
        "time_out_in_secs": system.config[
            "dag_runner_config", "real_time_loop_time_out_in_secs"
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
        "grid_time_in_secs": grid_time_in_secs,
    }
    # _LOG.debug("system=\n%s", str(system.config))
    dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
    return dag_runner


# #############################################################################
# Portfolio instances.
# #############################################################################


def get_DataFramePortfolio_from_System(
    system: dtfsyssyst.System,
) -> oms.Portfolio:
    event_loop = system.config["event_loop_object"]
    market_data = system.market_data
    asset_ids = system.config["market_data_config", "asset_ids"]
    portfolio = oms.get_DataFramePortfolio_example1(
        event_loop,
        market_data=market_data,
        # TODO(gp): These should go in the config.
        mark_to_market_col="close",
        pricing_method="twap.5T",
        asset_ids=asset_ids,
    )
    # TODO(gp): These should go in the config?
    portfolio.broker._column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    return portfolio


# TODO(Grisha): Generalize `get_DatabasePortfolio_from_System` and
#  `get_DataFramePortfolio_from_System`.
def get_DatabasePortfolio_from_System(
    system: dtfsyssyst.System,
) -> oms.Portfolio:
    event_loop = system.config["event_loop_object"]
    db_connection = system.config["db_connection_object"]
    market_data = system.market_data
    table_name = oms.CURRENT_POSITIONS_TABLE_NAME
    asset_ids = system.config["market_data_config", "asset_ids"]
    portfolio = oms.get_DatabasePortfolio_example1(
        event_loop,
        db_connection,
        table_name,
        market_data=market_data,
        # TODO(Grisha): These should go in the config as well as `_column_remap`.
        mark_to_market_col="close",
        pricing_method="twap.5T",
        asset_ids=asset_ids,
    )
    portfolio.broker._column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    return portfolio
