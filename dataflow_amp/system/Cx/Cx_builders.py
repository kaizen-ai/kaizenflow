"""
Import as:

import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
"""

import datetime
import logging
import os
from typing import Any, Callable, Dict

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)

# #############################################################################
# Market data instances
# #############################################################################


def get_Cx_HistoricalMarketData_example1(
    system: dtfsys.System,
) -> mdata.ImClientMarketData:
    """
    Build a `MarketData` client backed with the data defined by `ImClient`.
    """
    im_client = system.config["market_data_config", "im_client"]
    asset_ids = system.config["market_data_config", "asset_ids"]
    columns = None
    columns_remap = None
    # TODO(gp): Document why this is the wall clock time.
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    market_data = mdata.get_HistoricalImClientMarketData_example1(
        im_client,
        asset_ids,
        columns,
        columns_remap,
        wall_clock_time=wall_clock_time,
    )
    return market_data


def get_Cx_RealTimeMarketData_example1(
    system: dtfsys.System,
) -> mdata.MarketData:
    """
    Build a MarketData backed with RealTimeImClient.
    """
    # TODO(Grisha): @Dan pass as much as possible via `system.config`.
    resample_1min = False
    # Get environment variables with login info.
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    db_connection = hsql.get_connection(*connection_params)
    # Get the real-time `ImClient`.
    table_name = "ccxt_ohlcv"
    im_client = imvcdccccl.CcxtSqlRealTimeImClient(
        resample_1min, db_connection, table_name
    )
    # Get the real-time `MarketData`.
    event_loop = system.config["event_loop_object"]
    asset_ids = system.config["market_data_config", "asset_ids"]
    market_data, _ = mdata.get_RealTimeImClientMarketData_example1(
        im_client, event_loop, asset_ids
    )
    return market_data


# TODO(Grisha): @Dan share some code with `get_Cx_RealTimeMarketData_example1` but
# the difference will be that the prod `MarketData` should use the dev DB while
# `get_Cx_RealTimeMarketData_example1` should use the local DB.
def get_Cx_RealTimeMarketData_prod_instance1(
    system: dtfsys.System,
) -> mdata.MarketData:
    """
    Build a MarketData backed with RealTimeImClient.
    """
    # TODO(Grisha): @Dan pass as much as possible via `system.config`.
    resample_1min = False
    # Get environment variables with login info.
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    db_connection = hsql.get_connection(*connection_params)
    # Get the real-time `ImClient`.
    table_name = "ccxt_ohlcv"
    im_client = imvcdccccl.CcxtSqlRealTimeImClient(
        resample_1min, db_connection, table_name
    )
    # Get the real-time `MarketData`.
    asset_ids = system.config["market_data_config", "asset_ids"]
    market_data, _ = mdata.get_RealTimeImClientMarketData_example2(
        im_client, asset_ids
    )
    return market_data


# #############################################################################
# Process forecasts configs.
# #############################################################################


def get_Cx_process_forecasts_dict_example1(
    system: dtfsys.System,
) -> Dict[str, Any]:
    """
    Get the dictionary with `ProcessForecastsNode` config params for C1b
    pipeline.
    """
    prediction_col = "vwap.ret_0.vol_adj_2_hat"
    volatility_col = "vwap.ret_0.vol"
    spread_col = None
    order_duration_in_mins = 5
    style = "cross_sectional"
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "bulk_fill_method": "zero",
        "target_gmv": 1e5,
    }
    log_dir = None
    process_forecasts_dict = dtfsys.get_process_forecasts_dict_example1(
        system.portfolio,
        prediction_col,
        volatility_col,
        spread_col,
        order_duration_in_mins,
        style,
        compute_target_positions_kwargs,
        log_dir,
    )
    return process_forecasts_dict


def get_process_forecasts_dict_prod_instance1(
    portfolio: oms.Portfolio,
    order_duration_in_mins: int,
) -> Dict[str, Any]:
    """
    Build process forecast dictionary for a production system.
    """
    # prediction_col = "prediction"
    prediction_col = "vwap.ret_0.vol_adj_2_hat"
    volatility_col = "vwap.ret_0.vol"
    # spread_col = "pct_bar_spread"
    spread_col = None
    style = "cross_sectional"
    #
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "target_gmv": 2000.0,
    }
    log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    #
    process_forecasts_dict = dtfsys.get_process_forecasts_dict_example1(
        portfolio,
        prediction_col,
        volatility_col,
        spread_col,
        order_duration_in_mins,
        style,
        compute_target_positions_kwargs,
        log_dir=log_dir,
    )
    return process_forecasts_dict


# #############################################################################
# DAG instances.
# #############################################################################


def get_Cx_HistoricalDag_example1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG with a historical data source for simulation.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    # Create HistoricalDataSource.
    stage = "read_data"
    market_data = system.market_data
    # TODO(gp): This in the original code was
    # ts_col_name = "timestamp_db"
    ts_col_name = "end_ts"
    multiindex_output = True
    # col_names_to_remove = ["start_datetime", "timestamp_db"]
    col_names_to_remove = []
    node = dtfsys.HistoricalDataSource(
        stage,
        market_data,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Build the DAG.
    dag_builder = system.config["dag_builder_object"]
    dag = dag_builder.get_dag(system.config["dag_config"])
    # Add the data source node.
    dag.insert_at_head(node)
    return dag


def get_Cx_RealTimeDag_example1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    return dag


def get_Cx_RealTimeDag_example2(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource` and `ForecastProcessorNode`.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    # Configure a `ProcessForecastNode`.
    process_forecasts_config = get_Cx_process_forecasts_dict_example1(system)
    system.config[
        "process_forecasts_config"
    ] = cconfig.get_config_from_nested_dict(process_forecasts_config)
    # Append the `ProcessForecastNode`.
    dag = dtfsys.add_process_forecasts_node(system, dag)
    return dag


# TODO(gp): Copied from _get_E1_dag_prod... Try to share code.
def _get_Cx_dag_prod_instance1(
    system: dtfsys.System,
    get_process_forecasts_dict_func: Callable,
) -> dtfcore.DAG:
    """
    Build the DAG for a C1b production system from a system config.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    # Create the pipeline.
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    # TODO(gp): Fast prod system must be set before the DAG is built.
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
    # The config must be complete and stable here.
    dag = dag_builder.get_dag(dag_config)
    system = dtfsys.apply_dag_property(dag, system)
    #
    system = dtfsys.apply_dag_runner_config(system)
    # Build Portfolio.
    trading_period_str = dag_builder.get_trading_period(dag_config)
    # TODO(gp): Add a param to get_trading_period to return the int.
    order_duration_in_mins = int(trading_period_str.replace("T", ""))
    system.config[
        "portfolio_config", "order_duration_in_mins"
    ] = order_duration_in_mins
    portfolio = system.portfolio
    # Set market data history lookback in days in to config.
    system = dtfsys.apply_history_lookback(system)
    # Build the process forecast dict.
    process_forecasts_dict = get_process_forecasts_dict_func(
        portfolio, order_duration_in_mins
    )
    system.config[
        "process_forecasts_config"
    ] = cconfig.get_config_from_nested_dict(process_forecasts_dict)
    # Assemble.
    market_data = system.market_data
    market_data_history_lookback = system.config["market_data_config", "history_lookback"]
    ts_col_name = "timestamp_db"
    dag = dtfsys.adapt_dag_to_real_time(
        dag,
        market_data,
        market_data_history_lookback,
        process_forecasts_dict,
        ts_col_name,
    )
    _LOG.debug("dag=\n%s", dag)
    return dag


def get_Cx_dag_prod_instance1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a production system from a system config.
    """
    # TODO(gp): It seems that we inlined the code somewhere so we should factor it
    #  out.
    # get_process_forecasts_dict_func = dtfsys.get_process_forecasts_dict_example3
    get_process_forecasts_dict_func = get_process_forecasts_dict_prod_instance1
    dag = _get_Cx_dag_prod_instance1(system, get_process_forecasts_dict_func)
    return dag


# #############################################################################
# Portfolio instances.
# #############################################################################


def get_Cx_portfolio_prod_instance1(system: dtfsys.System) -> oms.Portfolio:
    """
    Build Portfolio instance for production.
    """
    market_data = system.market_data
    dag_builder = system.config["dag_builder_object"]
    trading_period_str = dag_builder.get_trading_period(
        system.config["dag_config"]
    )
    _LOG.debug(hprint.to_str("trading_period_str"))
    pricing_method = "twap." + trading_period_str
    portfolio = oms.get_CcxtPortfolio_prod_instance(
        system.config["cf_config", "strategy"],
        system.config["cf_config", "liveness"],
        system.config["cf_config", "instance_type"],
        market_data,
        system.config["market_data_config", "asset_ids"],
        system.config["portfolio_config", "order_duration_in_mins"],
        system.config["portfolio_config", "order_extra_params"],
        pricing_method,
    )
    return portfolio


# TODO(gp): We should dump the state of the portfolio and load it back.
# TODO(gp): Probably all prod system needs to have use_simulation and trade_date and
#  so we can generalize the class to be not E8 specific.
def _get_Cx_portfolio(
    system: dtfsys.System,
) -> oms.Portfolio:
    # We prefer to configure code statically (e.g., without switches) but in this
    # case the prod system vs its simulat-able version are so close (and we want to
    # keep them close) that we use a switch.
    if not system.use_simulation:
        # Prod.
        portfolio = get_Cx_portfolio_prod_instance1(system)
    else:
        # Simulation.
        # TODO(gp): This needs to be fixed before reconciliation.
        # _LOG.warning("Configuring for simulation")
        # portfolio = oms.get_DatabasePortfolio_example3(
        #     system.config["db_connection_object"],
        #     system.config["event_loop_object"],
        #     system.market_data,
        # )
        pass
    return portfolio
