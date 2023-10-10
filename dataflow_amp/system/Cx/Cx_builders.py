"""
Import as:

import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Union

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
import oms.broker.ccxt.ccxt_portfolio as obccccpo

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


def _get_ProcessForecastsNode_dict_instance1(
    system: dtfsys.System,
    order_config: Dict[str, Any],
    optimizer_config: Dict[str, Any],
    root_log_dir: str,
) -> Dict[str, Any]:
    """
    Build the `ProcessForecastsNode` dictionary for simulation.
    """
    spread_col = None
    dag_builder = system.config["dag_builder_object"]
    volatility_col = dag_builder.get_column_name("volatility")
    prediction_col = dag_builder.get_column_name("prediction")
    #
    process_forecasts_node_dict = dtfsys.get_ProcessForecastsNode_dict_example1(
        system.portfolio,
        prediction_col,
        volatility_col,
        spread_col,
        order_config,
        optimizer_config,
        root_log_dir,
    )
    return process_forecasts_node_dict


# #############################################################################
# DAG instances.
# #############################################################################


def _get_Cx_RealTimeDag(
    system: dtfsys.System,
    order_config: Dict[str, Any],
    optimizer_config: Dict[str, Any],
    root_log_dir: str,
    share_quantization: Optional[int],
) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource` and `ForecastProcessorNode`.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    # Configure a `ProcessForecastNode`.
    process_forecasts_node_dict = _get_ProcessForecastsNode_dict_instance1(
        system,
        order_config,
        optimizer_config,
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
    order_config = {
        "order_type": "price@twap",
        "passivity_factor": None,
        "order_duration_in_mins": 5,
    }
    #
    compute_target_positions_kwargs = {
        "bulk_frac_to_remove": 0.0,
        "bulk_fill_method": "zero",
        "target_gmv": 1e5,
    }
    optimizer_config = {
        "backend": "pomo",
        "params": {
            "style": "cross_sectional",
            "kwargs": compute_target_positions_kwargs,
        },
    }
    #
    root_log_dir = None
    # Round to a meaningful number of decimal places for the unit tests, otherwise
    # the division is performed differently on different machines, see CmTask4707.
    share_quantization = 9
    #
    dag = _get_Cx_RealTimeDag(
        system,
        order_config,
        optimizer_config,
        root_log_dir,
        share_quantization,
    )
    return dag


def get_Cx_dag_prod_instance1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a prod run from a system config.

    Compute target posisions style is "cross_sectional".
    """
    # TODO(Grisha): use `Config.get_and_mark_as_used()`.
    # Infer order and optimizer config values from `System`.
    order_config = system.config[
        "process_forecasts_node_dict",
        "process_forecasts_dict",
        "order_config",
    ]
    optimizer_config = system.config[
        "process_forecasts_node_dict",
        "process_forecasts_dict",
        "optimizer_config",
    ]
    # Convert to dict in order to comply with the required format.
    order_config = order_config.to_dict()
    optimizer_config = optimizer_config.to_dict()
    #
    root_log_dir = system.config.get_and_mark_as_used(
        ("system_log_dir"), default_value=None
    )
    share_quantization = None
    #
    dag = _get_Cx_RealTimeDag(
        system,
        order_config,
        optimizer_config,
        root_log_dir,
        share_quantization,
    )
    return dag


# TODO(Dan): Not used, deprecate.
# TODO(Grisha): somehow pass params via a cmd line instead of
# creating multiple prod instances.
def get_Cx_dag_prod_instance2(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a prod run from a system config.

    Compte target posisions style is "longitudinal".
    """
    order_config = {
        "order_type": "price@twap",
        "passivity_factor": None,
        "order_duration_in_mins": 5,
    }
    #
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
    optimizer_config = {
        "backend": "pomo",
        "params": {
            "style": "longitudinal",
            "kwargs": compute_target_positions_kwargs,
        },
    }
    #
    root_log_dir = system.config.get_and_mark_as_used(
        ("system_log_dir"), default_value=None
    )
    share_quantization = None
    #
    dag = _get_Cx_RealTimeDag(
        system,
        order_config,
        optimizer_config,
        root_log_dir,
        share_quantization,
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
) -> obccccpo.CcxtPortfolio:
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
    portfolio = obccccpo.get_CcxtPortfolio_prod_instance1(
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
