"""
Import as:

import dataflow.pipelines.examples.example1_configs as dtfpexexco
"""

import logging
from typing import List

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.pipelines.examples.example1_pipeline as dtfpexexpi
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdbg as hdbg
import market_data as mdata

_LOG = logging.getLogger(__name__)


def _build_base_config() -> cconfig.Config:
    backtest_config = cconfig.Config()
    # Save the `DagBuilder` and the `DagConfig` in the config.
    dag_builder = dtfpexexpi.Example1_DagBuilder()
    dag_config = dag_builder.get_config_template()
    backtest_config["DAG"] = dag_config
    backtest_config["meta", "dag_builder"] = dag_builder
    # backtest_config["tags"] = []
    return backtest_config


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
    egid_key = ("meta", "asset_ids")
    configs = dtfmoexcon.build_configs_varying_universe_tiles(
        config, egid_key, universe_tiles
    )
    return configs


# TODO(gp): Should we use a SystemRunner also here?
def get_dag_runner(config: cconfig.Config) -> dtfcore.AbstractDagRunner:
    """
    Build a DAG runner from a config.
    """
    # TODO(gp): In the previous code asset_ids was coming from:
    #  `asset_ids = dtfuniver.get_universe(universe_str)`.
    asset_ids = [3303714233, 1467591036]
    columns: List[str] = []
    columns_remap = None
    market_data = mdata.get_ImClientMarketData_example2(
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


# TODO(gp): This used to be:
#  `def build_tile_configs(experiment_config: str) -> List[cconfig.Config]:`.
def build_tile_configs(
    asset_ids: List[int],
    start_timestamp: str,
    end_timestamp: str,
) -> List[cconfig.Config]:
    """
    Build a tile configs for Example1 pipeline.
    """
    # TODO(gp): This used to be:
    #  (
    #      universe_str,
    #      trading_period_str,
    #      time_interval_str,
    #  ) = dtfmoexcon.parse_experiment_config(experiment_config).
    #
    # Apply specific config.
    # config = _apply_config(config, trading_period_str)
    #
    start_timestamp = pd.Timestamp(start_timestamp)
    end_timestamp = pd.Timestamp(end_timestamp)
    config = _build_base_config()
    #
    config["meta", "dag_runner"] = get_dag_runner
    # Name of the asset_ids to save.
    config["meta", "asset_id_col_name"] = "asset_id"
    configs = [config]
    # Apply the cross-product by the universe tiles.
    # TODO(gp): This used to be:
    #  `func = lambda cfg: build_configs_with_tiled_universe(cfg, universe_str)`.
    func = lambda cfg: build_configs_with_tiled_universe(cfg, asset_ids)
    configs = dtfmoexcon.apply_build_configs(func, configs)
    _LOG.info("After applying universe tiles: num_configs=%s", len(configs))
    hdbg.dassert_lte(1, len(configs))
    # Apply the cross-product by the time tiles.
    # TODO(gp): This is how timestamps were specified in the original code:
    #  `start_timestamp, end_timestamp = dtfmoexcon.get_period(time_interval_str)`.
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
