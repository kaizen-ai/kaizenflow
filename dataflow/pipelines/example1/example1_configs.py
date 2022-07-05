"""
Import as:

import dataflow.pipelines.example1.example1_configs as dtfpexexco
"""

import logging
from typing import List

import core.config as cconfig
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc

_LOG = logging.getLogger(__name__)


def build_tile_configs(
    system_config: cconfig.Config,
) -> List[cconfig.Config]:
    """
    Build a tile configs for Example1 pipeline.
    """
    configs = [system_config]
    universe_str = system_config["backtest_config"]["universe_str"]
    time_interval_str = system_config["backtest_config"]["time_interval_str"]
    # # TODO(gp): `trading_period_str` is not used for Example1 pipeline.
    # # Apply specific config.
    # # config = _apply_config(config, trading_period_str)
    # _ = trading_period_str
    # TODO(Grisha): do not specify `ImClient` twice, it is already specified
    #  in `market_data`.
    # Get universe from `ImClient` and convert it to asset ids.
    full_symbols = dtfuniver.get_universe(universe_str)
    im_client = icdc.get_DataFrameImClient_example1()
    asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
    system_config["market_data_config", "asset_ids"] = asset_ids
    # Name of the asset_ids to save.
    system_config["market_data_config", "asset_id_col_name"] = "asset_id"
    # #
    # config["dag_runner"] = get_dag_runner
    # # Create the list of configs.
    # configs = [config]
    # Apply the cross-product by the universe tiles.
    func = lambda cfg: dtfmoexcon.build_configs_with_tiled_universe(
        cfg, asset_ids
    )
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
    configs: List[cconfig.Config] = dtfmoexcon.apply_build_configs(func, configs)
    hdbg.dassert_lte(1, len(configs))
    _LOG.info("After applying time tiles: num_configs=%s", len(configs))
    return configs
