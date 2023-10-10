"""
Generate configs for execution analysis.

Import as:

import oms.execution_analysis_configs as oexancon
"""

import logging
from typing import List

import core.config as cconfig
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Master_execution_analysis
# #############################################################################


def get_execution_analysis_configs_Cmtask4881(
    system_log_dir: str,
) -> cconfig.ConfigList:
    """
    Build execution analysis config with default values and provided system log
    dir.
    """
    #
    id_col = "asset_id"
    universe_version = "v7.1"
    vendor = "CCXT"
    mode = "trade"
    test_asset_id = 1467591036
    bar_duration = "5T"
    expected_num_child_orders = [0, 5]
    use_historical = True
    config_list = build_execution_analysis_configs(
        system_log_dir,
        id_col,
        universe_version,
        vendor,
        mode,
        test_asset_id,
        bar_duration,
        expected_num_child_orders,
        use_historical,
    )
    return config_list


def build_execution_analysis_configs(
    system_log_dir: str,
    id_col: str,
    universe_version: str,
    vendor: str,
    mode: str,
    test_asset_id: int,
    bar_duration: str,
    expected_num_child_orders: List[int],
    use_historical: bool,
) -> cconfig.ConfigList:
    """
    Build configs for `Master_execution_analysis` notebook.

    :param system_log_dir: path to execution logs
    :param id_col: name of asset_id column, e.g. 'asset_id'
    :param universe_version: e.g. 'v7.1'
    :param vendor: vendor to load data for (e.g., CCXT, Talos)
    :param mode: download or trade universe
    :param test_asset_id: asset id to use as example, e.g. '1467591036'
    :param bar_duration: as pd.Timedelta-compatible string, e.g. '5T' for 5 minutes
    :param expected_num_child_orders: number of child orders we expect to
        execute per bar per parent order, as a list
        e.g. `[0,5]` for either 0 orders or 5 orders.
    :param use_historical: to use real-time or archived OHLCV data.
        Use 'True' for experiments older than 3 days, 'False' otherwise.
    :return: list of configs with a single resulting config
    """
    hdbg.dassert_path_exists(system_log_dir)
    hdbg.dassert_container_type(expected_num_child_orders, list, int)
    # Build the config.
    config_dict = {
        "meta": {"id_col": id_col, "use_historical": use_historical},
        "system_log_dir": system_log_dir,
        "ohlcv_market_data": {
            "vendor": vendor,
            "mode": mode,
            "universe": {
                "universe_version": universe_version,
                "test_asset_id": test_asset_id,
            },
        },
        "execution_parameters": {
            "bar_duration": bar_duration,
            "expected_num_child_orders_per_bar": expected_num_child_orders,
        },
    }
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list


# #############################################################################
# Master_bid_ask_execution_analysis
# #############################################################################


def get_bid_ask_execution_analysis_configs_Cmtask4881(
    system_log_dir: str,
) -> cconfig.ConfigList:
    """
    Build default config for `Master_bid_ask_execution_analysis` using real-
    time data with provided system log dir.
    """
    # TODO(Danya): Add new parameters to the config.
    # TODO(Danya): create general `get_bid_ask_execution_analysis_configs`.
    hdbg.dassert_path_exists(system_log_dir)
    use_historical = False
    config_dict = {
        "meta": {"use_historical": use_historical},
        "system_log_dir": system_log_dir,
    }
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list


# #############################################################################
# Master_broker_debugging
# #############################################################################


def get_broker_debugging_configs_Cmtask4881(
    system_log_dir: str,
) -> cconfig.ConfigList:
    """
    Build default config for `Master_broker_debugging` using real-time data
    with provided system log dir.
    """
    # TODO(Danya): Add new parameters to the config.
    hdbg.dassert_path_exists(system_log_dir)
    config_dict = {"system_log_dir": system_log_dir}
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list
