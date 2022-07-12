"""
Import as:

import dataflow.system.system_builder_utils as dtfssybuut
"""

import logging
from typing import Callable

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.system as dtfsyssyst
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc
import market_data as mdata

_LOG = logging.getLogger(__name__)


# #############################################################################
# System config utils
# #############################################################################


def get_system_config_template_from_dag_builder(
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
    ) = dtfmoexcon.parse_experiment_config(backtest_config)
    # Fill system config.
    hdbg.dassert_in(trading_period_str, ("1T", "5T", "15T"))
    system.config[
        "dag_config", "resample", "transformer_kwargs", "rule"
    ] = trading_period_str
    system.config["dag_runner_object"] = system.get_dag_runner
    system.config["backtest_config", "universe_str"] = universe_str
    system.config["backtest_config", "trading_period_str"] = trading_period_str
    system.config["backtest_config", "time_interval_str"] = time_interval_str
    return system


def apply_market_data_config(
    system: dtfsyssyst.ForecastSystem
) -> dtfsyssyst.ForecastSystem:
    """
    Convert full symbol universe to asset ids and fill market data config.
    """
    im_client = _build_im_client_from_config(system)
    universe_str = system.config["backtest_config", "universe_str"]
    full_symbols = dtfuniver.get_universe(universe_str)
    asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
    #
    system.config["market_data_config", "im_client"] = im_client
    system.config["market_data_config", "asset_ids"] = asset_ids
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    return system


def _build_im_client_from_config(system: dtfsyssyst.System) -> icdc.ImClient:
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


def get_event_loop_MarketData_from_df(
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
    market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        data,
    )
    return market_data


# #############################################################################
# Source node instances
# #############################################################################


# #############################################################################
# DAG building utils
# #############################################################################


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
    # This is for debugging. It saves the output of each node in a `csv` file.
    # dag.set_debug_mode("df_as_csv", False, "dst_dir")
    if False:
        dag.force_free_nodes = True
    return dag


# #############################################################################
# DAG runner instances.
# #############################################################################


# TODO(gp): @all -> get_RealtimeDagRunner or get_RealtimeDagRunner_from_system?
def get_realtime_DagRunner_from_system(
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
