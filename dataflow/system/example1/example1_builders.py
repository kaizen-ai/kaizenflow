"""
Import as:

import dataflow.system.example1.example1_builders as dtfsexexbu
"""

import datetime
import logging

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore

# TODO(gp): We can't use dtfsys because we are inside dataflow/system.
#  Consider moving out Example1 from this dir somehow so that we can use dtfsys
#  like we do for other systems.
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_builder_utils as dtfssybuut
import helpers.hdbg as hdbg
import market_data as mdata

_LOG = logging.getLogger(__name__)

# TODO(gp): -> example1_system_builder.py? What's the convention now?

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


def get_Example1_HistoricalDag_example1(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with `HistoricalDataSource` for simulation.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # Create HistoricalDataSource.
    stage = "read_data"
    market_data = system.market_data
    # TODO(gp): This in the original code was
    #  `ts_col_name = "timestamp_db"`.
    ts_col_name = "end_datetime"
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


def get_Example1_RealtimeDag_example2(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource`.
    """
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    # How much history is needed for the DAG to compute.
    lookback_in_days = 1
    system = dtfssybuut.apply_history_lookback(system, days=lookback_in_days)
    dag = dtfssybuut.add_real_time_data_source(system)
    return dag


def get_Example1_RealtimeDag_example3(system: dtfsyssyst.System) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource` and `ForecastProcessor`.
    """
    # How much history is needed for the DAG to compute.
    lookback_in_days = 7
    system = dtfssybuut.apply_history_lookback(system, days=lookback_in_days)
    dag = dtfssybuut.add_real_time_data_source(system)
    # Copied from E8_system_example.py
    # Configure a `ProcessForecast` node.
    # TODO(gp): @all we should use get_process_forecasts_dict_example1 or a similar
    #  function.
    prediction_col = "feature1"
    volatility_col = "vwap.ret_0.vol"
    spread_col = None
    bulk_frac_to_remove = 0.0
    target_gmv = 1e5
    log_dir = None
    # log_dir = os.path.join("process_forecasts", datetime.date.today().isoformat())
    order_type = "price@twap"
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
    process_forecasts_config = {
        "prediction_col": prediction_col,
        "volatility_col": volatility_col,
        "spread_col": spread_col,
        "portfolio": system.portfolio,
        "process_forecasts_config": process_forecasts_config_dict,
    }
    system.config[
        "process_forecasts_config"
    ] = cconfig.get_config_from_nested_dict(process_forecasts_config)
    # Append the ProcessForecast node.
    stage = "process_forecasts"
    _LOG.debug("stage=%s", stage)
    node = dtfsysinod.ProcessForecastsNode(
        stage, **system.config["process_forecasts_config"].to_dict()
    )
    dag.append_to_tail(node)
    return dag
