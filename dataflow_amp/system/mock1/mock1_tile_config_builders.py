"""
Import as:

import dataflow_amp.system.mock1.mock1_tile_config_builders as dtfasmmtcbu
"""

import logging

import dataflow.system as dtfsys
import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy

_LOG = logging.getLogger(__name__)


def build_Mock1_tile_config_list(
    backtest_config: str,
) -> dtfsys.SystemConfigList:
    system = dtfasmmfosy.get_Mock1_ForecastSystem_for_simulation_example1(
        backtest_config
    )
    system_config_list = dtfsys.build_tile_config_list(system)
    return system_config_list
