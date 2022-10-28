"""
Import as:

import oms.reconciliation as omreconc
"""

import datetime
import logging
import os
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hpickle as hpickle
import helpers.hsystem as hsystem
import oms.ccxt_broker as occxbrok
import oms.portfolio as omportfo
import oms.target_position_and_order_generator as otpaorge

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


def load_target_positions(
    target_position_dir: str,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    freq: str,
    normalize_bar_times: bool,
) -> pd.DataFrame:
    """
    Load a target position dataframe.
    """
    # TODO(Paul): Share code with `load_portfolio_artifacts()`.
    # Make sure the directory exists.
    hdbg.dassert_dir_exists(target_position_dir)
    # Sanity-check timestamps.
    hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
    hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
    hdbg.dassert_lt(start_timestamp, end_timestamp)
    # Load the target position dataframe.
    target_position_df = (
        otpaorge.TargetPositionAndOrderGenerator.load_target_positions(
            target_position_dir
        )
    )
    # Sanity-check the dataframe.
    hpandas.dassert_time_indexed_df(
        target_position_df, allow_empty=False, strictly_increasing=True
    )
    # Sanity-check the date ranges of the dataframes against the start and
    # end timestamps.
    first_timestamp = target_position_df.index[0]
    _LOG.debug("First target_position_df timestamp=%s", first_timestamp)
    hdbg.dassert_lte(first_timestamp.round(freq), start_timestamp)
    last_timestamp = target_position_df.index[-1]
    _LOG.debug("Last target_position_df timestamp=%s", last_timestamp)
    hdbg.dassert_lte(end_timestamp, last_timestamp.round(freq))
    # Maybe normalize the bar times to `freq` grid.
    if normalize_bar_times:
        _LOG.debug("Normalizing bar times to %s grid", freq)
        target_position_df.index = target_position_df.index.round(freq)
    # Time-localize the target position dataframe.
    _LOG.debug(
        "Trimming times to start_timestamp=%s, end_timestamp=%s",
        start_timestamp,
        end_timestamp,
    )
    target_position_df = target_position_df.loc[start_timestamp:end_timestamp]
    #
    return target_position_df


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


def compute_share_prices_and_slippage(
    df: pd.DataFrame,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Compare trade prices against benchmark.

    :param df: a portfolio-like dataframe, with the following columns for
        each asset:
        - holdings_notional
        - holdings_shares
        - executed_trades_notional
        - executed_trades_shares
    :return: dataframe with per-asset
        - holdings_price_per_share
        - trade_price_per_share
        - slippage_in_bps
        - is_benchmark_profitable
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_eq(2, df.columns.nlevels)
    cols = [
        "holdings_notional",
        "holdings_shares",
        "executed_trades_notional",
        "executed_trades_shares",
    ]
    hdbg.dassert_is_subset(cols, df.columns.levels[0])
    # Compute price per share of holdings (using holdings reference price).
    # We assume that holdings are computed with a benchmark price (e.g., TWAP).
    holdings_price_per_share = df["holdings_notional"] / df["holdings_shares"]
    # We do not expect negative prices.
    hdbg.dassert_lte(0, holdings_price_per_share.min().min())
    # Compute price per share of trades (using execution reference prices).
    trade_price_per_share = (
        df["executed_trades_notional"] / df["executed_trades_shares"]
    )
    hdbg.dassert_lte(0, trade_price_per_share.min().min())
    # Buy = +1, sell = -1.
    buy = (df["executed_trades_notional"] > 0).astype(int)
    sell = (df["executed_trades_notional"] < 0).astype(int)
    side = buy - sell
    # Compute slippage against benchmark.
    slippage = (
        side
        * (trade_price_per_share - holdings_price_per_share)
        / holdings_price_per_share
    )
    slippage_in_bps = 1e4 * slippage
    # Determine whether the trade, if closed at t+1, would be profitable if
    # executed at the benchmark price on both legs.
    is_benchmark_profitable = side * np.sign(
        holdings_price_per_share.diff().shift(-1)
    )
    benchmark_return_in_bps = (
        1e4 * side * holdings_price_per_share.pct_change().shift(-1)
    )
    price_df = pd.concat(
        {
            "holdings_price_per_share": holdings_price_per_share,
            "trade_price_per_share": trade_price_per_share,
            "slippage_in_bps": slippage_in_bps,
            "benchmark_return_in_bps": benchmark_return_in_bps,
            "is_benchmark_profitable": is_benchmark_profitable,
        },
        axis=1,
    )
    if join_output_with_input:
        price_df = pd.concat([df, price_df], axis=1)
    return price_df


def get_asset_slice(df: pd.DataFrame, asset_id: int) -> pd.DataFrame:
    hpandas.dassert_time_indexed_df(
        df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_eq(2, df.columns.nlevels)
    hdbg.dassert_in(asset_id, df.columns.levels[1])
    slice_ = df.T.xs(asset_id, level=1).T
    return slice_


def compute_fill_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare targets to realized.
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_eq(2, df.columns.nlevels)
    cols = [
        "holdings_shares",
        "target_holdings_shares",
        "target_trades_shares",
    ]
    hdbg.dassert_is_subset(cols, df.columns.levels[0])
    # The trades and shares are signed to indicate the side.
    realized_trades_shares = df["holdings_shares"].subtract(
        df["holdings_shares"].shift(1), fill_value=0
    )
    # These are end-of-bar time-indexed.
    fill_rate = (
        realized_trades_shares / df["target_trades_shares"].shift(1)
    ).abs()
    tracking_error_shares = df["holdings_shares"] - df[
        "target_holdings_shares"
    ].shift(1)
    underfill_share_count = (
        df["target_trades_shares"].shift(1).abs() - realized_trades_shares.abs()
    )
    tracking_error_notional = df["holdings_notional"] - df[
        "target_holdings_notional"
    ].shift(1)
    tracking_error_bps = (
        1e4 * tracking_error_notional / df["target_holdings_notional"].shift(1)
    )
    #
    fills_df = pd.concat(
        {
            "realized_trades_shares": realized_trades_shares,
            "fill_rate": fill_rate,
            "underfill_share_count": underfill_share_count,
            "tracking_error_shares": tracking_error_shares,
            "tracking_error_notional": tracking_error_notional,
            "tracking_error_bps": tracking_error_bps,
        },
        axis=1,
    )
    return fills_df


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
        asset_id_to_share_decimals = occxbrok.subset_market_info(
            market_info, "amount_precision"
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


# #############################################################################
# Loading utils
# #############################################################################


def load_portfolio_dfs(
    portfolio_path_dict: Dict[str, str],
    portfolio_config: Dict[str, Any],
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Load multiple portfolios and portfolio stats from disk.

    :param portfolio_path_dict: paths to portfolios for different experiments
    :param portfolio_config: params for `load_portfolio_artifacts()`
    :return: portfolios and portfolio stats for different experiments
    """
    portfolio_dfs = {}
    portfolio_stats_dfs = {}
    for name, path in portfolio_path_dict.items():
        hdbg.dassert_path_exists(path)
        _LOG.info("Processing portfolio=%s path=%s", name, path)
        portfolio_df, portfolio_stats_df = load_portfolio_artifacts(
            path,
            **portfolio_config,
        )
        portfolio_dfs[name] = portfolio_df
        portfolio_stats_dfs[name] = portfolio_stats_df
    #
    return portfolio_dfs, portfolio_stats_dfs


def get_system_log_paths(
    system_log_path_dict: Dict[str, str], data_type: str
) -> Dict[str, str]:
    """
    Get paths to data inside a system log dir.

    :param system_log_path_dict: system log dirs paths for different experiments, e.g.,
        `{"prod": "/shared_data/system_log_dir", "sim": ...}`
    :param data_type: either "dag" to load DAG output or "portfolio" to load Portfolio
    :return: dir paths inside system log dir for different experiments, e.g.,
        `{"prod": "/shared_data/system_log_dir/process_forecasts/portfolio", "sim": ...}`
    """
    data_path_dict = {}
    if data_type == "portfolio":
        dir_name = "process_forecasts/portfolio"
    elif data_type == "dag":
        dir_name = "dag/node_io/node_io.data"
    else:
        raise ValueError(f"Unsupported data type={data_type}")
    for k, v in system_log_path_dict.items():
        cur_dir = os.path.join(v, dir_name)
        hdbg.dassert_dir_exists(cur_dir)
        data_path_dict[k] = cur_dir
    return data_path_dict


def get_latest_output_from_last_dag_node(dag_dir: str) -> pd.DataFrame:
    """
    Retrieve the most recent output from the last DAG node.

    This function relies on our file naming conventions.
    """
    hdbg.dassert_dir_exists(dag_dir)
    parquet_files = list(
        filter(lambda x: "parquet" in x, sorted(os.listdir(dag_dir)))
    )
    _LOG.info("Tail of files found=%s", parquet_files[-3:])
    file_name = parquet_files[-1]
    dag_parquet_path = os.path.join(dag_dir, file_name)
    _LOG.info("DAG parquet path=%s", dag_parquet_path)
    dag_df = hparque.from_parquet(dag_parquet_path)
    return dag_df


def load_config_from_pickle(
    system_log_path_dict: Dict[str, str]
) -> Dict[str, cconfig.Config]:
    """
    Load configs from pickle files given a dict of paths.
    """
    config_dict = {}
    file_name = "system_config.input.values_as_strings.pkl"
    for stage, path in system_log_path_dict.items():
        path = os.path.join(path, file_name)
        hdbg.dassert_path_exists(path)
        _LOG.debug("Reading config from %s", path)
        config_pkl = hpickle.from_pickle(path)
        config = cconfig.Config.from_dict(config_pkl)
        config_dict[stage] = config
    return config_dict


def compute_maximum_delay(df: pd.DataFrame, *, run_date: Optional[datetime.date] = None) -> None:
    if run_date:
        # Filter data frame by date.
        next_date = datetime.date(year=run_date.year, month=run_date.month, day=(run_date.day + 1))
        df = df[run_date:next_date]
    df.groupby(by=["full_symbol"]).max()["delta"].sort_values(ascending=False).plot(
    kind="bar")
