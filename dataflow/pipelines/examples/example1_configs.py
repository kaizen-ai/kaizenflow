"""
Import as:

import dataflow.pipelines.examples.example1_configs as dtfpexexco
"""

import logging
from typing import List

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.pipelines.examples.example1_pipeline as dtfpexexpi
import dataflow.system.source_nodes as dtfsysonod
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc
import market_data as mdata

_LOG = logging.getLogger(__name__)


# TODO(gp): We should unify with `ForecastSystem`. A `System` contains all the
# info to build and run a DAG and then it can be simulated or put in prod.
def _build_base_config() -> cconfig.Config:
    backtest_config = cconfig.Config()
    # Save the `DagBuilder` and the `DagConfig` in the config.
    dag_builder = dtfpexexpi.Example1_DagBuilder()
    dag_config = dag_builder.get_config_template()
    backtest_config["DAG"] = dag_config
    backtest_config["meta", "dag_builder"] = dag_builder
    # backtest_config["tags"] = []
    return backtest_config


# TODO(gp): @grisha. Centralize this.
def build_configs_with_tiled_universe(
    config: cconfig.Config, asset_ids: List[int]
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s tiled by universe.
    """
    if len(asset_ids) > 300:
        # if len(asset_ids) > 1000:
        # Split the universe in 2 parts.
        # TODO(gp): We can generalize this.
        split_idx = int(len(asset_ids) / 2)
        asset_ids_part1 = asset_ids[:split_idx]
        asset_ids_part2 = asset_ids[split_idx:]
        #
        universe_tiles = (asset_ids_part1, asset_ids_part2)
    else:
        universe_tiles = (asset_ids,)
    asset_id_key = ("meta", "asset_ids")
    configs = dtfmoexcon.build_configs_varying_universe_tiles(
        config, asset_id_key, universe_tiles
    )
    return configs


# TODO(gp): This corresponds to `System.get_dag_runner()`.
def get_dag_runner(config: cconfig.Config) -> dtfcore.AbstractDagRunner:
    """
    Build a DAG runner from a config.
    """
    asset_ids = config["meta", "asset_ids"]
    columns: List[str] = []
    columns_remap = None
    market_data = mdata.get_DataFrameImClientMarketData_example1(
        asset_ids, columns, columns_remap
    )
    # Create HistoricalDataSource.
    stage = "read_data"
    asset_id_col = "asset_id"
    # TODO(gp): This in the original code was
    #  `ts_col_name = "timestamp_db"`.
    ts_col_name = "end_ts"
    multiindex_output = True
    # col_names_to_remove = ["start_datetime", "timestamp_db"]
    col_names_to_remove = ["start_ts"]
    node = dtfsysonod.HistoricalDataSource(
        stage,
        market_data,
        asset_id_col,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Build the DAG.
    dag_builder = config["meta", "dag_builder"]
    dag = dag_builder.get_dag(config["DAG"])
    # This is for debugging. It saves the output of each node in a `csv` file.
    # dag.set_debug_mode("df_as_csv", False, "crypto_forever")
    if False:
        dag.force_freeing_nodes = True
    # Add the data source node.
    dag.insert_at_head(stage, node)
    # Build the DagRunner.
    dag_runner = dtfcore.FitPredictDagRunner(config, dag)
    return dag_runner


def build_tile_configs(experiment_config: str) -> List[cconfig.Config]:
    """
    Build a tile configs for Example1 pipeline.
    """
    (
        universe_str,
        trading_period_str,
        time_interval_str,
    ) = dtfmoexcon.parse_experiment_config(experiment_config)
    #
    config = _build_base_config()
    # TODO(gp): `trading_period_str` is not used for Example1 pipeline.
    # Apply specific config.
    # config = _apply_config(config, trading_period_str)
    # TODO(Grisha): do not specify `ImClient` twice, it is already specified
    # in `market_data`.
    # Get universe from `ImClient` and convert it to asset ids.
    full_symbols = dtfuniver.get_universe(universe_str)
    im_client = icdc.get_DataFrameImClient_example1()
    asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
    #
    config["meta", "dag_runner"] = get_dag_runner
    # Name of the asset_ids to save.
    config["meta", "asset_id_col_name"] = "asset_id"
    # Create the list of configs.
    configs = [config]
    # Apply the cross-product by the universe tiles.
    func = lambda cfg: build_configs_with_tiled_universe(cfg, asset_ids)
    configs = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying universe tiles: num_configs=%s", len(configs))
    hdbg.dassert_lte(1, len(configs))
    # Apply the cross-product by the time tiles.
    start_timestamp, end_timestamp = dtfmoexcon.get_period(time_interval_str)
    freq_as_pd_str = "M"
    # Amount of history fed to the DAG.
    lookback_as_pd_str = "10D"
    func = lambda cfg: dtfmoexcon.build_configs_varying_tiled_periods(
        cfg, start_timestamp, end_timestamp, freq_as_pd_str, lookback_as_pd_str
    )
    configs = dtfmoexcon.apply_build_configs(func, configs)
    hdbg.dassert_lte(1, len(configs))
    _LOG.info("After applying time tiles: num_configs=%s", len(configs))
    return configs
