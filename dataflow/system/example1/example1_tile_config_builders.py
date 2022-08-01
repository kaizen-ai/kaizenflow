"""
Import as:

import dataflow.system.example1.example1_tile_config_builders as dtfseetcobu
"""

import logging

import core.config.config_list_builder as cccolibu
import dataflow.system.example1.example1_forecast_system as dtfseefosy
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_config_list as dtfssycoli
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def build_Example1_tile_config_list(
    backtest_config: str,
) -> dtfssycoli.SystemConfigList:
    system = dtfseefosy.get_Example1_ForecastSystem_for_simulation_example1(
        backtest_config
    )
    hdbg.dassert_isinstance(system, dtfsyssyst.System)
    system_config_list = dtfssycoli.SystemConfigList.from_system(system)
    #
    system_config_list = (
        cccolibu.build_config_list_with_tiled_universe_and_periods(
            system_config_list
        )
    )
    hdbg.dassert_isinstance(system_config_list, dtfssycoli.SystemConfigList)
    return system_config_list
