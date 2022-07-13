"""
Import as:

import dataflow.system.example1.example1_builders as dtfsexexbu
"""

import logging

import pandas as pd

import dataflow.core as dtfcore
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_builder_utils as dtfssybuut
import helpers.hdbg as hdbg
import market_data as mdata

_LOG = logging.getLogger(__name__)


# #############################################################################
# Market data instances
# #############################################################################


def get_Example1_MarketData_example2(
    system: dtfsyssyst.System,
) -> mdata.ImClientMarketData:
    """
    Build a replayed MarketData from an ImClient feeding data from a df.
    """
    im_client = system.config["market_data_config", "im_client"]
    asset_ids = system.config["market_data_config", "asset_ids"]
    # TODO(gp): Specify only the columns that are needed.
    columns = None
    columns_remap = None
    market_data = mdata.get_HistoricalImClientMarketData_example1(
        im_client, asset_ids, columns, columns_remap
    )
    return market_data


# #############################################################################
# DAG instances.
# #############################################################################


def get_Example1_dag_example1(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with a historical data source for simulation.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # Create HistoricalDataSource.
    stage = "read_data"
    market_data = system.market_data
    # TODO(gp): This in the original code was
    #  `ts_col_name = "timestamp_db"`.
    ts_col_name = "end_ts"
    multiindex_output = True
    col_names_to_remove = ["start_ts"]
    node = dtfsysonod.HistoricalDataSource(
        stage,
        market_data,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    dag = dtfssybuut.build_dag_with_data_source_node(system, node)
    return dag


def get_Example1_realtime_dag_example1(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # Create RealTimeDataSource.
    stage = "read_data"
    market_data = system.market_data
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    ts_col_name = "end_ts"
    # How much history is needed for the DAG to compute.
    timedelta = pd.Timedelta("20T")
    node = dtfsysonod.RealTimeDataSource(
        stage,
        market_data,
        timedelta,
        ts_col_name,
        multiindex_output,
    )
    dag = dtfssybuut.build_dag_with_data_source_node(system, node)
    return dag


def get_Example1_dag_example3(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source and forecast processor.
    """
    stage = "load_prices"
    # How much history is needed for the DAG to compute.
    # TODO(gp): This should be
    # 198     system_config[
    # 199         "market_data_config", "history_lookback"
    # 200     ] = market_data_history_lookback
    timedelta = pd.Timedelta("7D")
    ts_col_name = "end_ts"
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    node = dtfsysonod.RealTimeDataSource(
        stage,
        system.market_data,
        ts_col_name,
        timedelta,
        multiindex_output,
    )
    dag = dtfssybuut.build_dag_with_data_source_node(system, node)
    # Copied from E8_system_example.py
    # Configure a `ProcessForecast` node.
    prediction_col = "feature1"
    volatility_col = "vwap.ret_0.vol"
    spread_col = None
    bulk_frac_to_remove = 0.0
    target_gmv = 1e5
    log_dir = None
    # log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    order_type = "price@twap"
    evaluate_forecasts_config = None
    process_forecasts_config_dict = {
        "order_config": {
            "order_type": order_type,
            "order_duration": 5,
        },
        "optimizer_config": {
            "backend": "pomo",
            "bulk_frac_to_remove": bulk_frac_to_remove,
            "bulk_fill_method": "zero",
            "target_gmv": target_gmv,
        },
        # TODO(gp): Use datetime.time()
        "ath_start_time": pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        ).time(),
        "trading_start_time": pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        ).time(),
        "ath_end_time": pd.Timestamp(
            "2000-01-01 16:40:00-05:00", tz="America/New_York"
        ).time(),
        "trading_end_time": pd.Timestamp(
            "2000-01-01 16:40:00-05:00", tz="America/New_York"
        ).time(),
        "execution_mode": "real_time",
        "log_dir": log_dir,
    }
    system.config["process_forecasts_config"] = {
        "prediction_col": prediction_col,
        "volatility_col": volatility_col,
        "spread_col": spread_col,
        "portfolio": system.portfolio,
        "process_forecasts_config": process_forecasts_config_dict,
        "evaluate_forecasts_config": evaluate_forecasts_config,
    }
    # Append the ProcessForecast node.
    stage = "process_forecasts"
    _LOG.debug("stage=%s", stage)
    node = dtfsysinod.ProcessForecasts(
        stage, **system.config["process_forecasts_config"]
    )
    dag.append_to_tail(node)
    return dag
