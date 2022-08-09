"""
Import as:

import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
"""

import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import dataflow.system.system as dtfsyssyst
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.data.client as icdc
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


def get_RealTimeImClientMarketData_prod_instance1(
    im_client: icdc.ImClient,
    asset_ids: List[int],
) -> Tuple[mdata.MarketData, hdateti.GetWallClockTime]:
    """
    Build a `RealTimeMarketData` for production.
    """
    asset_id_col = "asset_id"
    start_time_col_name = "start_timestamp"
    end_time_col_name = "end_timestamp"
    columns = None
    event_loop = None
    get_wall_clock_time = lambda: hdateti.get_current_time(
        tz="ET", event_loop=event_loop
    )
    # 
    market_data = mdata.RealTimeMarketData2(
        im_client,
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
    )
    return market_data, get_wall_clock_time


# #############################################################################
# Process forecasts configs.
# #############################################################################


def get_Cx_process_forecasts_dict_example1(
    system: dtfsyssyst.System,
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
