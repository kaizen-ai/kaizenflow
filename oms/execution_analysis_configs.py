"""
Generate configs for execution analysis.

Import as:

import oms.execution_analysis_configs as oexancon
"""

import logging

import core.config as cconfig
import helpers.hdbg as hdbg
import reconciliation as reconcil

_LOG = logging.getLogger(__name__)


# #############################################################################
# Master_execution_analysis
# #############################################################################


def get_execution_analysis_configs_Cmtask4881(
    system_log_dir: str,
    bar_duration: str,
    universe_version: str,
) -> cconfig.ConfigList:
    """
    Build execution analysis config with default values and provided system log
    dir.

    :param system_log_dir: dir where run logs are saved,
      e.g., ../system_log_dir.manual/process_forecasts
    :param bar_duration: length of bar in time, e.g., `5T`
    :param universe_version: version of the universe, e.g., `v7.4`
    """
    #
    id_col = "asset_id"
    vendor = "CCXT"
    mode = "trade"
    test_asset_id = 1464553467
    child_order_execution_freq = "1T"
    use_historical = True
    config_list = build_execution_analysis_configs(
        system_log_dir,
        id_col,
        universe_version,
        vendor,
        mode,
        test_asset_id,
        bar_duration,
        child_order_execution_freq,
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
    child_order_execution_freq: str,
    use_historical: bool,
) -> cconfig.ConfigList:
    """
    Build configs for `Master_execution_analysis` notebook.

    :param system_log_dir: path to execution logs
    :param id_col: name of asset_id column, e.g. 'asset_id'
    :param universe_version: e.g. 'v7.4'
    :param vendor: vendor to load data for (e.g., CCXT)
    :param mode: download or trade universe
    :param test_asset_id: asset id to use as example, e.g. '1467591036'
    :param bar_duration: as pd.Timedelta-compatible string, e.g. '5T'
        for 5 minutes
    :param child_order_execution_freq: execution frequency of child
        orders, e.g. 1T or 30s
    :param use_historical: to use real-time or archived OHLCV data. Use
        'True' for experiments older than 3 days, 'False' otherwise.
    :return: list of configs with a single resulting config
    """
    hdbg.dassert_path_exists(system_log_dir)
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
            "execution_freq": child_order_execution_freq,
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
    bar_duration: str,
    *,
    test_asset_id: int = 1464553467,
) -> cconfig.ConfigList:
    """
    Build default config for `Master_bid_ask_execution_analysis` using real-
    time data with provided system log dir.
    """
    hdbg.dassert_path_exists(system_log_dir)
    use_historical = False
    config_dict = {
        "meta": {"use_historical": use_historical},
        "universe": {"test_asset_id": test_asset_id},
        "execution_parameters": {"bar_duration": bar_duration},
        "system_log_dir": system_log_dir,
    }
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list


def get_bid_ask_execution_analysis_configs(
    system_log_dir: str,
    bar_duration: str,
    *,
    test_asset_id: int = 1464553467,
    use_historical: bool = False,
) -> cconfig.ConfigList:
    """
    Build default config for `Master_bid_ask_execution_analysis` using real-
    time data with provided system log dir.

    :param system_log_dir: path to execution logs
    :param test_asset_id: asset id to use as example
    :param bar_duration: as pd.Timedelta-compatible string, e.g. '5T'
    :param use_historical: to use real-time or archived OHLCV data. Use
        'True' for experiments older than 3 days, 'False' otherwise.
    """
    hdbg.dassert_path_exists(system_log_dir)
    config_dict = {
        "meta": {"use_historical": use_historical},
        "universe": {"test_asset_id": test_asset_id},
        "execution_parameters": {"bar_duration": bar_duration},
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


# #############################################################################
# Master_broker_portfolio_reconciliation
# #############################################################################


def build_broker_portfolio_reconciliation_configs(
    system_log_dir: str,
    id_col: str,
    universe_version: str,
    price_column_name: str,
    vendor: str,
    mode: str,
    bar_duration: str,
) -> cconfig.ConfigList:
    """
    Build configs for `Master_broker_portfolio_reconciliation` notebook.

    :param system_log_dir: path to execution logs
    :param id_col: name of asset_id column, e.g. "asset_id"
    :param universe_version: e.g. "v7.4"
    :param price_column_name: name of price column, e.g., "close"
    :param vendor: vendor to load data for (e.g., CCXT)
    :param mode: download or trade universe
    :param bar_duration: as pd.Timedelta-compatible string, e.g. "5T"
        for 5 minutes
    :return: list of configs with a single resulting config
    """
    hdbg.dassert_path_exists(system_log_dir)
    # Build the config.
    config_dict = {
        "id_col": id_col,
        "system_log_dir": system_log_dir,
        "ohlcv_market_data": {
            "vendor": vendor,
            "mode": mode,
            "universe_version": universe_version,
        },
        # TODO(Nina): extract from `SystemConfig`.
        "price_column_name": price_column_name,
        "bar_duration": bar_duration,
        "share_asset_ids_with_no_fills": 0.3,
        "n_index_elements_to_ignore": 2,
        "target_positions_columns_to_compare": [
            "price",
            "holdings_shares",
            "holdings_notional",
            "target_holdings_shares",
            "target_holdings_notional",
            "target_trades_shares",
            "target_trades_notional",
        ],
        "compare_dfs_kwargs": {
            "row_mode": "inner",
            "column_mode": "inner",
            "diff_mode": "pct_change",
            "assert_diff_threshold": 1e-3,
            "log_level": logging.INFO,
        },
    }
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list


# TODO(Grisha): Consider moving universe version and bar duration extraction
# to `build_broker_portfolio_reconciliation_configs()` and killing
# the current function.
def get_broker_portfolio_reconciliation_configs_Cmtask5690(
    system_log_dir: str,
) -> cconfig.ConfigList:
    """
    Build default config for `Master_broker_portfolio_reconciliation` using
    real-time data with provided system log dir.
    """
    id_col = "asset_id"
    system_config_dir = system_log_dir.rstrip("/process_forecasts")
    universe_version = reconcil.extract_universe_version_from_pkl_config(
        system_config_dir
    )
    price_column_name = "close"
    vendor = "CCXT"
    mode = "trade"
    bar_duration = reconcil.extract_bar_duration_from_pkl_config(
        system_config_dir
    )
    config_list = build_broker_portfolio_reconciliation_configs(
        system_log_dir,
        id_col,
        universe_version,
        price_column_name,
        vendor,
        mode,
        bar_duration,
    )
    return config_list
