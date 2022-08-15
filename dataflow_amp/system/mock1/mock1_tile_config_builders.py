"""
Import as:

import dataflow_amp.system.mock1.mock1_tile_config_builders as dtfasmmtcbu
"""

import logging

import core.config.config_list_builder as cccolibu
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_config_list as dtfssycoli
import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def build_Mock1_tile_config_list(
    backtest_config: str,
) -> dtfssycoli.SystemConfigList:
    system = dtfasmmfosy.get_Mock1_ForecastSystem_for_simulation_example1(
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
