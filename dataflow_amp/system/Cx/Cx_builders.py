"""
Import as:

import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Union

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.db.db_utils as imvcddbut
import market_data as mdata

# TODO(Sonya): it needs to be just `import oms`, but avoiding potential
# circular imports: https://github.com/cryptokaizen/cmamp/issues/4510.
import oms.ccxt.ccxt_portfolio as occccpor

_LOG = logging.getLogger(__name__)

# TODO(gp): @all -> Cx_system_builders.py


# #############################################################################
# Market data instances.
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


def get_Cx_RealTimeMarketData_prod_instance1(
    asset_ids: List[int],
) -> mdata.MarketData:
    """
    Build a `MarketData` backed with `RealTimeImClient`.
    """
    _LOG.debug(hprint.to_str("asset_ids"))
    universe_version = "infer_from_data"
    # Get DB connection.
    db_connection = imvcddbut.DbConnectionManager.get_connection("prod")
    # Get the real-time `ImClient`.
    table_name = "ccxt_ohlcv_futures"
    # TODO(Grisha): @Dan pass as much as possible via `system.config`.
    resample_1min = False
    im_client = imvcdccccl.CcxtSqlRealTimeImClient(
        universe_version, db_connection, table_name, resample_1min=resample_1min
    )
    # Get the real-time `MarketData`.
    market_data, _ = mdata.get_RealTimeImClientMarketData_example1(
        im_client, asset_ids
    )
    return market_data


# #############################################################################
# Process forecasts configs.
# #############################################################################


def get_Cx_order_config_prod_instance1(
    system: dtfsys.System,
    order_duration_in_mins: int,
) -> Dict[str, Any]:
    """
    Get the order config prod instance.

    :param system: the `System` class
    :param order_duration_in_mins: see `TargetPositionAndOrderGenerator`
    :return: config for placing the orders, e.g.,
        ```
        order_type: price@custom_twap
        passivity_factor: 0.55
        order_duration_in_mins: 5
        ```
    """
    # Run mode is specific of the `Cx_ProdSystem` and is not used in other cases.
    if "run_mode" in system.config:
        run_mode = system.config.get("run_mode")
        if run_mode == "prod":
            # Place limit orders in the TWAP fashion.
            order_type = "price@custom_twap"
            passivity_factor = 0.55
        elif run_mode in ["paper_trading", "simulation"]:
            # The Broker is mocked. Thus, it does not matter which order type to use
            # since there is no real execution.
            order_type = "price@twap"
            passivity_factor = None
        else:
            raise ValueError(f"Unsupported run_mode={run_mode}")
    else:
        # The params below are irrelevant for the unit tests since there is no
        # interaction with an exchange. It is ok to use the market order type.
        order_type = "price@twap"
        passivity_factor = None
    order_config = {
        "order_type": order_type,
        "passivity_factor": passivity_factor,
        "order_duration_in_mins": order_duration_in_mins,
    }
    return order_config


# TODO(Grisha): separate the unit test instance from the
# prod instance.
def get_ProcessForecastsNode_dict_instance1(
    system: dtfsys.System,
    order_duration_in_mins: int,
    optimizer_backend: str,
    style: str,
    compute_target_positions_kwargs: Any,
    root_log_dir: str,
) -> Dict[str, Any]:
    """
    Build the `ProcessForecastsNode` dictionary for simulation.
    """
    spread_col = None
    dag_builder = system.config["dag_builder_object"]
    volatility_col = dag_builder.get_column_name("volatility")
    prediction_col = dag_builder.get_column_name("prediction")
    # Get the order config.
    order_config = get_Cx_order_config_prod_instance1(
        system, order_duration_in_mins
    )
    #
    process_forecasts_node_dict = dtfsys.get_ProcessForecastsNode_dict_example1(
        system.portfolio,
        prediction_col,
        volatility_col,
        spread_col,
        order_config,
        style,
        optimizer_backend,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    return process_forecasts_node_dict


# #############################################################################
# DAG instances.
# #############################################################################


def _get_Cx_RealTimeDag(
    system: dtfsys.System,
    share_quantization: str,
    optimizer_backend: str,
    style: str,
    compute_target_positions_kwargs: Any,
    root_log_dir: str,
) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource` and `ForecastProcessorNode`.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag_builder = system.config.get_and_mark_as_used(
        "dag_builder_object", mark_key_as_used=False
    )
    dag_config = system.config.get_and_mark_as_used(
        "dag_config", mark_key_as_used=False
    )
    dag = dtfsys.add_real_time_data_source(system)
    # Configure a `ProcessForecastNode`.
    mark_key_as_used = True
    trading_period_str = dag_builder.get_trading_period(
        dag_config, mark_key_as_used
    )
    # TODO(gp): Add a param to `get_trading_period()` to return the int.
    order_duration_in_mins = int(trading_period_str.replace("T", ""))
    process_forecasts_node_dict = get_ProcessForecastsNode_dict_instance1(
        system,
        order_duration_in_mins,
        optimizer_backend,
        style,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    system.config["process_forecasts_node_dict"] = cconfig.Config.from_dict(
        process_forecasts_node_dict
    )
    system = dtfsys.apply_ProcessForecastsNode_config_for_crypto(
        system, share_quantization
    )
    # Append the `ProcessForecastNode`.
    dag = dtfsys.add_ProcessForecastsNode(system, dag)
    return dag


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
    Build a DAG with `RealTimeDataSource` for a unit tests from a system
    config.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    return dag


def get_Cx_RealTimeDag_example2(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG for unit tests with `RealTimeDataSource` and
    `ForecastProcessorNode` from a system config.
    """
    share_quantization = "no_quantization"
    optimizer_backend = "pomo"
    style = "cross_sectional"
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "bulk_fill_method": "zero",
        "target_gmv": 1e5,
    }
    root_log_dir = None
    dag = _get_Cx_RealTimeDag(
        system,
        share_quantization,
        optimizer_backend,
        style,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    return dag


def get_Cx_dag_prod_instance1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a prod run from a system config.

    Compute target posisions style is "cross_sectional".
    """
    share_quantization = "asset_specific"
    # TODO(Grisha): use `Config.get_and_mark_as_used()`.
    # Infer the optimizer config values from `System`.
    optimizer_backend = system.config[
        "process_forecasts_node_dict",
        "process_forecasts_dict",
        "optimizer_config",
        "backend",
    ]
    style = system.config[
        "process_forecasts_node_dict",
        "process_forecasts_dict",
        "optimizer_config",
        "params",
        "style",
    ]
    compute_target_positions_kwargs = system.config[
        "process_forecasts_node_dict",
        "process_forecasts_dict",
        "optimizer_config",
        "params",
        "kwargs",
    ]
    root_log_dir = system.config.get_and_mark_as_used(
        ("system_log_dir"), default_value=None
    )
    dag = _get_Cx_RealTimeDag(
        system,
        share_quantization,
        optimizer_backend,
        style,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    return dag


# TODO(Grisha): somehow pass params via a cmd line instead of
# creating multiple prod instances.
def get_Cx_dag_prod_instance2(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a prod run from a system config.

    Compte target posisions style is "longitudinal".
    """
    share_quantization = "asset_specific"
    optimizer_backend = "cc_pomo"
    style = "longitudinal"
    compute_target_positions_kwargs = {
        "prediction_abs_threshold": 0.0,
        "volatility_to_spread_threshold": 0.0,
        "volatility_lower_bound": 1e-4,
        "gamma": 0.0,
        "target_dollar_risk_per_name": 1.0,
        "spread_lower_bound": 1e-4,
        "modulate_using_prediction_magnitude": False,
        "constant_decorrelation_coefficient": 0.0,
    }
    root_log_dir = system.config.get_and_mark_as_used(
        ("system_log_dir"), default_value=None
    )
    dag = _get_Cx_RealTimeDag(
        system,
        share_quantization,
        optimizer_backend,
        style,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    return dag


# #############################################################################
# Portfolio instances.
# #############################################################################


# TODO(gp): We should dump the state of the portfolio and load it back.
# TODO(gp): Probably all prod system needs to have run_mode and trade_date and
#  so we can generalize the class to be not E8 specific.
def get_Cx_portfolio_prod_instance1(
    system: dtfsys.System,
) -> occccpor.CcxtPortfolio:
    """
    Build Portfolio instance for production.
    """
    run_mode = system.config.get_and_mark_as_used("run_mode")
    #
    market_data = system.market_data
    system = dtfsys.apply_Portfolio_config(system)
    column_remap = system.config.get_and_mark_as_used(
        ("portfolio_config", "column_remap")
    )
    asset_ids = system.config.get_and_mark_as_used(
        ("market_data_config", "asset_ids")
    )
    cf_config_strategy = system.config.get_and_mark_as_used(
        ("cf_config", "strategy"), default_value="Cx"
    )
    universe_version = system.config.get_and_mark_as_used(
        ("market_data_config", "universe_version"), default_value=None
    )
    secret_identifier_config = system.config.get_and_mark_as_used(
        "secret_identifier_config", default_value=None
    )
    log_dir = system.config.get_and_mark_as_used("system_log_dir")
    log_dir = os.path.join(log_dir, "process_forecasts")
    #
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    mark_key_as_used = True
    trading_period_str = dag_builder.get_trading_period(
        dag_config, mark_key_as_used
    )
    _LOG.debug(hprint.to_str("trading_period_str"))
    pricing_method = "twap." + trading_period_str
    #
    portfolio = occccpor.get_CcxtPortfolio_prod_instance1(
        run_mode,
        cf_config_strategy,
        market_data,
        column_remap,
        universe_version,
        secret_identifier_config,
        pricing_method,
        asset_ids,
        log_dir,
    )
    return portfolio


# #############################################################################
# Apply config utils.
# #############################################################################


# TODO(Grisha): P1, consider removing.
def apply_Cx_MarketData_config(
    system: dtfsys.System,
    replayed_delay_in_mins_or_timestamp: Union[int, pd.Timestamp],
) -> dtfsys.System:
    """
    Extend system config with parameters for `MarketData` init.
    """
    system.config["market_data_config", "datetime_columns"] = [
        "start_datetime",
        "end_datetime",
        "timestamp_db",
    ]
    system.config["market_data_config", "timestamp_db_column"] = "end_datetime"
    system.config["market_data_config", "aws_profile"] = "ck"
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 10
    system.config[
        "market_data_config", "replayed_delay_in_mins_or_timestamp"
    ] = replayed_delay_in_mins_or_timestamp
    return system


# TODO(Grisha): P1, consider removing.
def apply_Cx_DagRunner_config(
    system: dtfsys.System,
    rt_timeout_in_secs_or_time: Union[int, datetime.time],
) -> dtfsys.System:
    """
    Extend system config with parameters for `DagRunner` init.
    """
    # TODO(Grisha): @Dan Factor out to `get_trading_period_from_system()`.
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    mark_key_as_used = False
    trading_period = dag_builder.get_trading_period(dag_config, mark_key_as_used)
    system.config["dag_runner_config", "bar_duration_in_secs"] = int(
        pd.Timedelta(trading_period).total_seconds()
    )
    if hasattr(dag_builder, "fit_at_beginning"):
        system.config[
            "dag_runner_config", "fit_at_beginning"
        ] = dag_builder.fit_at_beginning
    system.config[
        "dag_runner_config", "rt_timeout_in_secs_or_time"
    ] = rt_timeout_in_secs_or_time
    return system
