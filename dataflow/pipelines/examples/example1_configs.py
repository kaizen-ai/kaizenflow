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
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg

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


# TODO(gp): Generalize this by passing asset_ids.
def build_configs_with_tiled_universe(
    config: cconfig.Config, universe_str: str
) -> List[cconfig.Config]:
    """
    Create a list of `Config`s tiled by universe.
    """
    asset_ids = dtfuniver.get_universe(universe_str)
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


def get_dag_runner(config: cconfig.Config) -> dtfcore.AbstractDagRunner:
    """
    Build a DAG runner from a config.
    """
    # Build the DAG.
    dag_builder = config["meta", "dag_builder"]
    dag = dag_builder.get_dag(config["DAG"])
    # Add the data source node.
    # dag.insert_at_head(stage, node)
    # Build the DagRunner.
    dag_runner = dtfcore.FitPredictDagRunner(config, dag)
    return dag_runner


def build_tile_configs(
    experiment_config: str,
) -> List[cconfig.Config]:
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
    # Apply specific config.
    # config = _apply_config(config, trading_period_str)
    #
    config["meta", "dag_runner"] = get_dag_runner
    # Name of the asset_ids to save.
    config["meta", "asset_id_col_name"] = "asset_id"
    configs = [config]
    # Apply the cross-product by the universe tiles.
    func = lambda cfg: build_configs_with_tiled_universe(cfg, universe_str)
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
