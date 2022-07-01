"""
Import as:

import dataflow.system.example1.example1_builders as dtfsexexbu
"""

import logging

import pandas as pd

import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system as dtfsyssyst
import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc
import market_data as mdata

_LOG = logging.getLogger(__name__)


# #############################################################################
# Market data instances
# #############################################################################


def get_Example1_market_data_example2(
    system: dtfsyssyst.System,
) -> mdata.ReplayedMarketData:
    """
    Build a replayed MarketData from an ImClient feeding data from a df.
    """
    asset_ids = system.config["market_data_config", "asset_ids"]
    # TODO(gp): Specify only the columns that are needed.
    columns = None
    columns_remap = None
    im_client = icdc.get_DataFrameImClient_example1()
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
    asset_id_col = "asset_id"
    # TODO(gp): This in the original code was
    #  `ts_col_name = "timestamp_db"`.
    ts_col_name = "end_ts"
    multiindex_output = True
    col_names_to_remove = ["start_ts"]
    node = dtfsysonod.HistoricalDataSource(
        stage,
        market_data,
        asset_id_col,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    dag = _build_dag_with_data_source_node(system, node)
    return dag


def get_Example1_dag_example2(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # Create RealTimeDataSource.
    stage = "read_data"
    market_data = system.market_data
    asset_id_col = "asset_id"
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    # How much history is needed for the DAG to compute.
    timedelta = pd.Timedelta("20T")
    node = dtfsysonod.RealTimeDataSource(
        stage,
        market_data,
        timedelta,
        asset_id_col,
        multiindex_output,
    )
    dag = _build_dag_with_data_source_node(system, node)
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
    asset_id_col = "asset_id"
    # The DAG works on multi-index dataframe containing multiple
    # features for multiple assets.
    multiindex_output = True
    node = dtfsysonod.RealTimeDataSource(
        stage,
        system.market_data,
        timedelta,
        asset_id_col,
        multiindex_output,
    )
    dag = _build_dag_with_data_source_node(system, node)
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


def _build_dag_with_data_source_node(
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
    # This is for debugging. It saves the output of each node in a `csv` file.
    # dag.set_debug_mode("df_as_csv", False, "dst_dir")
    if False:
        dag.force_free_nodes = True
    return dag


# #############################################################################
# DAG runner instances.
# #############################################################################


# TODO(gp): -> example1_system_example.py
#
# TODO(gp): -> get_Example1_dag_runner_example2
# TODO(gp): Is this the same as get_Example1_dag_runner?
# TODO(gp): -> example1_system_example.py
def get_Example1_dag_runner_example1(
    system: dtfsyssyst.System,
) -> dtfsrtdaru.RealTimeDagRunner:
    """
    Build a real-time DAG runner.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    dag = system.dag
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
