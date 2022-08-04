"""
Import as:

import dataflow_orange.system.C1b.C1b_builders as dtfoscc1bu
"""

import datetime
import logging

import pandas as pd

import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata

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
    # This is for debugging. It saves the output of each node in a `csv` file.
    # dag.set_debug_mode("df_as_csv", False, "crypto_forever")
    if False:
        dag.force_freeing_nodes = True
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
    Build a DAG with a real time data source and forecast processor.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    # Copied from E8_system_example.py
    # Configure a `ProcessForecast` node.
    # TODO(gp): @all we should use get_process_forecasts_dict_example1 or a similar
    #  function.
    prediction_col = system.config["research_pnl", "prediction_col"]
    volatility_col = system.config["research_pnl", "volatility_col"]
    spread_col = None
    bulk_frac_to_remove = 0.0
    target_gmv = 1e5
    log_dir = None
    # log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    order_type = "price@twap"
    forecast_evaluator_from_prices_dict = None
    process_forecasts_config_dict = {
        "order_config": {
            "order_type": order_type,
            "order_duration_in_mins": 5,
        },
        "optimizer_config": {
            "backend": "pomo",
            "params": {
                "style": "cross_sectional",
                "kwargs": {
                    "bulk_frac_to_remove": bulk_frac_to_remove,
                    "bulk_fill_method": "zero",
                    "target_gmv": target_gmv,
                },
            },
        },
        "ath_start_time": datetime.time(9, 30),
        "trading_start_time": datetime.time(9, 30),
        "ath_end_time": datetime.time(16, 40),
        "trading_end_time": datetime.time(16, 40),
        "execution_mode": "real_time",
        "log_dir": log_dir,
    }
    system.config["process_forecasts_config"] = {
        "prediction_col": prediction_col,
        "volatility_col": volatility_col,
        "spread_col": spread_col,
        "portfolio": system.portfolio,
        "process_forecasts_config": process_forecasts_config_dict,
        "forecast_evaluator_from_prices_dict": forecast_evaluator_from_prices_dict,
    }
    # Append the ProcessForecast node.
    stage = "process_forecasts"
    _LOG.debug("stage=%s", stage)
    node = dtfsys.ProcessForecasts(
        stage, **system.config["process_forecasts_config"]
    )
    dag.append_to_tail(node)
    return dag
