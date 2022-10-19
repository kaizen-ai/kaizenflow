"""
Import as:

import oms.reconciliation as omreconc
"""

import datetime
import logging
import os
from typing import Tuple

import numpy as np
import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hsystem as hsystem
import oms.ccxt_broker as occxbrok
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def load_portfolio_artifacts(
    portfolio_dir: str,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    freq: str,
    normalize_bar_times: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load a portfolio dataframe and its associated stats dataframe.

    :return: portfolio_df, portfolio_stats_df
    """
    # Make sure the directory exists.
    hdbg.dassert_dir_exists(portfolio_dir)
    # Sanity-check timestamps.
    hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
    hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
    hdbg.dassert_lt(start_timestamp, end_timestamp)
    # Load the portfolio and stats dataframes.
    portfolio_df, portfolio_stats_df = omportfo.Portfolio.read_state(
        portfolio_dir,
    )
    # Sanity-check the dataframes.
    hpandas.dassert_time_indexed_df(
        portfolio_df, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        portfolio_stats_df, allow_empty=False, strictly_increasing=True
    )
    # Sanity-check the date ranges of the dataframes against the start and
    # end timestamps.
    first_timestamp = portfolio_df.index[0]
    _LOG.debug("First portfolio_df timestamp=%s", first_timestamp)
    hdbg.dassert_lte(first_timestamp.round(freq), start_timestamp)
    last_timestamp = portfolio_df.index[-1]
    _LOG.debug("Last portfolio_df timestamp=%s", last_timestamp)
    hdbg.dassert_lte(end_timestamp, last_timestamp.round(freq))
    # Maybe normalize the bar times to `freq` grid.
    if normalize_bar_times:
        _LOG.debug("Normalizing bar times to %s grid", freq)
        portfolio_df.index = portfolio_df.index.round(freq)
        portfolio_stats_df.index = portfolio_stats_df.index.round(freq)
    # Time-localize the portfolio dataframe and portfolio stats dataframe.
    _LOG.debug(
        "Trimming times to start_timestamp=%s, end_timestamp=%s",
        start_timestamp,
        end_timestamp,
    )
    portfolio_df = portfolio_df.loc[start_timestamp:end_timestamp]
    portfolio_stats_df = portfolio_stats_df.loc[start_timestamp:end_timestamp]
    #
    return portfolio_df, portfolio_stats_df


def normalize_portfolio_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df.drop(-1, axis=1, level=1, inplace=True)
    return normalized_df


def compute_delay(df: pd.DataFrame, freq: str) -> pd.Series:
    bar_index = df.index.round(freq)
    delay_vals = df.index - bar_index
    delay = pd.Series(delay_vals, bar_index, name="delay")
    return delay


def compute_shares_traded(
    portfolio_df: pd.DataFrame,
    order_df: pd.DataFrame,
    freq: str,
) -> pd.DataFrame:
    """
    Compute the number of shares traded between portfolio snapshots.

    :param portfolio_df: dataframe reconstructed from logged `Portfolio`
        object
    :param order_df: dataframe constructed from logged `Order` objects
    :freq: bar frequency for dataframe index rounding (for bar alignment and
        easy merging)
    :return: multilevel column dataframe with shares traded, targets,
        estimated benchmark cost per share, and underfill counts
    """
    # Process `portfolio_df`.
    hdbg.dassert_isinstance(portfolio_df, pd.DataFrame)
    hdbg.dassert_in("executed_trades_shares", portfolio_df.columns)
    hdbg.dassert_in("executed_trades_notional", portfolio_df.columns)
    portfolio_df.index = portfolio_df.index.round(freq)
    executed_trades_shares = portfolio_df["executed_trades_shares"]
    executed_trades_notional = portfolio_df["executed_trades_notional"]
    asset_ids = executed_trades_shares.columns
    # Divide the notional flow (signed) by the shares traded (signed)
    # to get the estimated (positive) price at which the trades took place.
    executed_trades_price_per_share = executed_trades_notional.abs().divide(
        executed_trades_shares
    )
    # Process `order_df`.
    hdbg.dassert_isinstance(order_df, pd.DataFrame)
    hdbg.dassert_is_subset(
        ["end_timestamp", "asset_id", "diff_num_shares"], order_df.columns
    )
    # Pivot the order dataframe.
    order_share_targets = order_df.pivot(
        index="end_timestamp",
        columns="asset_id",
        values="diff_num_shares",
    )
    order_share_targets.index = order_share_targets.index.round(freq)
    # Compute underfills.
    share_target_sign = np.sign(order_share_targets)
    underfill = share_target_sign * (order_share_targets - executed_trades_shares)
    # Combine into a multi-column dataframe.
    df = pd.concat(
        {
            "shares_traded": executed_trades_shares,
            "order_share_target": order_share_targets,
            "executed_trades_price_per_shares": executed_trades_price_per_share,
            "underfill": underfill,
        },
        axis=1,
    )
    # The indices may not perfectly agree in the concat, and so we perform
    # another fillna and int casting.
    df["underfill"] = df["underfill"].fillna(0).astype(int)
    return df


# #############################################################################
# Reconciliation config
# #############################################################################


def build_reconciliation_configs() -> cconfig.ConfigList:
    """
    Build reconciliation configs that are specific of an asset class.
    """
    # Infer the meta-parameters from env.
    date_key = "AM_RECONCILIATION_DATE"
    if date_key in os.environ:
        date_str = os.environ[date_key]
    else:
        date_str = datetime.date.today().strftime("%Y%m%d")
    #
    timeout_key = "RT_TIMEOUT_IN_SECS_OR_TIME"
    if timeout_key in os.environ:
        rt_timeout_in_secs_or_time = os.environ[timeout_key]
    else:
        rt_timeout_in_secs_or_time = 2 * 60 * 60
    #
    asset_key = "AM_ASSET_CLASS"
    if asset_key in os.environ:
        asset_class = os.environ[asset_key]
    else:
        asset_class = "crypto"
    # Set values for variables that are specific of an asset class.
    if asset_class == "crypto":
        # For crypto the TCA part is not implemented yet.
        run_tca = False
        #
        bar_duration = "5T"
        #
        root_dir = "/shared_data/prod_reconciliation"
        # Prod system is run via AirFlow and the results are tagged with the previous day.
        previous_day_date_str = (
            pd.Timestamp(date_str) - pd.Timedelta("1D")
        ).strftime("%Y-%m-%d")
        prod_dir = os.path.join(
            root_dir,
            date_str,
            "prod",
            f"system_log_dir_scheduled__{previous_day_date_str}T10:00:00+00:00_2hours",
        )
        system_log_path_dict = {
            "prod": prod_dir,
            # For crypto we do not have a `candidate` so we just re-use prod.
            "cand": prod_dir,
            "sim": os.path.join(
                root_dir, date_str, "simulation", "system_log_dir"
            ),
        }
        #
        fep_init_dict = {
            "price_col": "vwap",
            "prediction_col": "vwap.ret_0.vol_adj_2_hat",
            "volatility_col": "vwap.ret_0.vol",
        }
        quantization = "asset_specific"
        market_info = occxbrok.load_market_data_info()
        asset_id_to_share_decimals = (
            occxbrok.subset_market_info(
                market_info, "amount_precision"
            )
        )
        gmv = 700.0
        liquidate_at_end_of_day = False
    elif asset_class == "equities":
        run_tca = True
        #
        bar_duration = "15T"
        #
        root_dir = ""
        search_str = ""
        prod_dir_cmd = f"find {root_dir}/{date_str}/prod -name '{search_str}'"
        _, prod_dir = hsystem.system_to_string(prod_dir_cmd)
        cand_cmd = (
            f"find {root_dir}/{date_str}/job.candidate.* -name '{search_str}'"
        )
        _, cand_dir = hsystem.system_to_string(cand_cmd)
        system_log_path_dict = {
            "prod": prod_dir,
            "cand": cand_dir,
            "sim": os.path.join(root_dir, date_str, "system_log_dir"),
        }
        #
        fep_init_dict = {
            "price_col": "twap",
            "prediction_col": "prediction",
            "volatility_col": "garman_klass_vol",
        }
        quantization = "nearest_share"
        asset_id_to_share_decimals = None
        gmv = 20000.0
        liquidate_at_end_of_day = True
    else:
        raise ValueError(f"Unsupported asset class={asset_class}")
    # Sanity check dirs.
    for dir in system_log_path_dict.values():
        hdbg.dassert_dir_exists(dir)
    # Build the config.
    config_dict = {
        "meta": {
            "date_str": date_str,
            "rt_timeout_in_secs_or_time": rt_timeout_in_secs_or_time,
            "asset_class": asset_class,
            "run_tca": run_tca,
            "bar_duration": bar_duration,
        },
        "system_log_path": system_log_path_dict,
        "research_forecast_evaluator_from_prices": {
            "init": fep_init_dict,
            "annotate_forecasts_kwargs": {
                "quantization": quantization,
                "asset_id_to_share_decimals": asset_id_to_share_decimals,
                "burn_in_bars": 3,
                "style": "cross_sectional",
                "bulk_frac_to_remove": 0.0,
                "target_gmv": gmv,
                "liquidate_at_end_of_day": liquidate_at_end_of_day,
            },
        },
    }
    config = cconfig.Config.from_dict(config_dict)
    config_list = cconfig.ConfigList([config])
    return config_list
