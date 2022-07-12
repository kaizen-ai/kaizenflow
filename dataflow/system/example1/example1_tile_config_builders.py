"""
Import as:

import dataflow.system.example1.example1_tile_config_builders as dtfseetcobu
"""

import logging
from typing import List

import core.config as cconfig
import dataflow.model.experiment_config as dtfmoexcon
import dataflow.system.example1.example1_forecast_system as dtfseefosy

_LOG = logging.getLogger(__name__)


def build_Example1_tile_configs(backtest_config: str) -> List[cconfig.Config]:
    system = dtfseefosy.get_Example1_ForecastSystem_example1(backtest_config)
    system_configs = dtfmoexcon.build_configs_with_tiled_universe_and_periods(
        system.config
    )
    return system_configs
