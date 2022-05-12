"""
Import as:

import core.finance.tradability as cfintrad
"""


import random
from typing import Dict

import numpy as np
import pandas as pd
import statsmodels
from numpy.typing import ArrayLike

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import logging

import matplotlib.pyplot as plt
import pandas as pd

import core.config.config_ as cconconf
import core.finance as cofinanc
import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import core.plotting.normality as cplonorm
import core.plotting.plotting_utils as cplpluti
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdbg as hdbg
import helpers.hprint as hprint


def process_df(df: pd.DataFrame, freq_mins: int) -> pd.DataFrame:
    rics = df["ric"].unique()
    hdbg.dassert_eq(len(rics), 1, "Found more than one ric: %s", rics)
    ric = rics[0]
    #
    egids = df["egid"].unique()
    hdbg.dassert_eq(len(egids), 1, "Found more than one egid: %s", egids)
    egid = egids[0]
    #
    out_df = pd.DataFrame()
    out_df["close"] = df["close"].resample(f"{freq_mins}T").last()
    out_df["ric"] = ric
    out_df["egid"] = egid
    out_df["volume"] = df["volume"].resample(f"{freq_mins}T").sum()
    # # TODO(gp): This should be VWAP.
    out_df["bid"] = df["bid"].resample(f"{freq_mins}T").mean()
    out_df["ask"] = df["ask"].resample(f"{freq_mins}T").mean()
    out_df = out_df.dropna()
    # #
    out_df["ret_0"] = out_df["close"].pct_change()
    out_df["spread_usd"] = out_df["ask"] - out_df["bid"]
    out_df["spread_bps"] = (out_df["ask"] - out_df["bid"]) / out_df["close"]
    out_df["trad"] = out_df["ret_0"].abs() / out_df["spread_bps"]
    out_df = out_df.dropna()
    return out_df


def compute_stats(df: pd.DataFrame) -> pd.Series:
    rics = df["ric"].unique()
    hdbg.dassert_eq(len(rics), 1, "Found more than 1 future: %s", rics)
    ric = rics[0]
    srs = {
        "ric": ric,
        "close.mean": df["close"].mean(),
        "ret_0.mad": df["ret_0"].mad(),
        "spread_usd.median": df["spread_usd"].median(),
        "spread_bps.median": df["spread_bps"].median(),
        "trad": df["trad"].median(),
    }
    srs = pd.Series(srs)
    return srs


# #############################################################################


def get_predictions(
    df: pd.DataFrame, ret_col: str, hit_rate: float, seed: int
) -> pd.Series:
    """
    :param df: Desired sample with OHLCV data and calculated returns
    :param hit_rate: Desired percantage of successful predictions
    :param seed: Experiment stance
    """
    hdbg.dassert_lte(0, hit_rate)
    hdbg.dassert_lte(hit_rate, 1)
    random.seed(seed)
    n = df.shape[0]
    rets = df[ret_col].values
    # Mask contains 1 for a desired hit and -1 for a miss.
    num_hits = int((1 - hit_rate) * n)
    mask = ([-1] * num_hits) + ([1] * (n - num_hits))
    # print(len(mask))
    mask = np.asarray(mask)
    # Randomize the location of the outcomes.
    random.shuffle(mask)
    # Construct predictions using the sign of the actual returns.
    pred = pd.Series(np.sign(rets) * mask)
    # Change the index for easy attachment to initial DataFrame.
    pred.index = df.index
    return pred


def calculate_confidence_interval(
    hit_series: pd.Series, alpha: float, method: str
) -> None:
    """
    :param hit_series: boolean series with hit values
    :param alpha: Significance level
    :param method: "normal", "agresti_coull", "beta", "wilson", "binom_test"
    """
    point_estimate = hit_series.mean()
    hit_lower, hit_upper = statsmodels.stats.proportion.proportion_confint(
        count=hit_series.sum(),
        nobs=hit_series.count(),
        alpha=alpha,
        method=method,
    )
    result_values_pct = [100 * point_estimate, 100 * hit_lower, 100 * hit_upper]
    conf_alpha = (1 - alpha / 2) * 100
    print(f"hit_rate: {result_values_pct[0]}")
    print(f"hit_rate_lower_CI_({conf_alpha}%): {result_values_pct[1]}")
    print(f"hit_rate_upper_CI_({conf_alpha}%): {result_values_pct[2]}")


def get_predictions_and_hits(df, ret_col, hit_rate, seed):
    """
    Calculate hits from the predictions and show confidence intervals.

    :param df: Desired sample with OHLCV data and calculated returns
    :param ret_col: Name of the column with returns
    :param hit_rate: Desired percentage of successful predictions
    :param seed: Experiment stance
    """
    df = df.copy()
    df["predictions"] = get_predictions(df, ret_col, hit_rate, seed)
    # Specify necessary columns.
    df = df[[ret_col, "predictions"]]
    # Attach `hit` column (boolean).
    df = df.copy()
    df["hit"] = df[ret_col] * df["predictions"] >= 0
    # Exclude NaNs for the better analysis (at least one in the beginning because of `pct_change()`)
    df = hpandas.dropna(df, report_stats=False)
    return df


def compute_pnl(df: pd.DataFrame, rets_col: str) -> float:
    return (df["predictions"] * df[rets_col]).sum()


def simulate_pnls_for_set_of_hit_rates(
    df: pd.DataFrame, rets_col: str, hit_rates: ArrayLike, n_experiment: int
) -> Dict[float, float]:
    """
    For the set of various pre-defined `hit_rates` values iterate several
    generations for the actual PnL.

    :param df: Desired sample with calculated returns
    :param rets_col: Name of the column with returns
    :param hit_rates: Set of hit rates for the experiment
    :param n_experiment: Number of iterations for each `hit_rate`
    :return: Corresponding `PnL` for each `hit_rate`
    """
    # Every seed corresponds to a different "model".
    seed = random.randint(0, 100)
    # Placeholder for the results.
    results = {}
    # Each value of hit rate is making its own PnL value.
    for hit_rate in hit_rates:
        # For each value of hit rate produce `n_experiment` iterations.
        for i in range(n_experiment):
            # Generate predictions and hits for a given `hit_rate`.
            df_tmp = get_predictions_and_hits(df, rets_col, hit_rate, seed)
            # The actual `hit_rate`.
            hit_rate = df_tmp["hit"].mean()
            # The actual `PnL`.
            pnl = compute_pnl(df_tmp, rets_col)
            # Attach corresponding `hit_rate` and `PnL` to the dictionary.
            results[hit_rate] = pnl
            # Reassign seed value.
            seed += 1
    return results


# #############################################################################


def calculate_vwap_twap(df: pd.DataFrame, resampling_rule: str) -> pd.DataFrame:
    """
    Resample the data and calculate VWAP, TWAP using DataFlow methods.

    :param df: Raw data
    :param resampling_rule: Desired resampling frequency
    :return: Resampled multiindex DataFrame with computed metrics
    """
    # Configure the node to do the TWAP / VWAP resampling.
    node_resampling_config = {
        "in_col_groups": [
            ("close",),
            ("volume",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "rule": resampling_rule,
            "resampling_groups": [
                ({"close": "close"}, "last", {}),
                (
                    {
                        "close": "twap",
                    },
                    "mean",
                    {},
                ),
                (
                    {
                        "volume": "volume",
                    },
                    "sum",
                    {"min_count": 1},
                ),
            ],
            "vwap_groups": [
                ("close", "volume", "vwap"),
            ],
        },
        "reindex_like_input": False,
        "join_output_with_input": False,
    }
    # Put the data in the DataFlow format (which is multi-index).
    converted_data = dtfsysonod._convert_to_multiindex(df, "full_symbol")
    # Create the node.
    nid = "resample"
    node = dtfcore.GroupedColDfToDfTransformer(
        nid,
        transformer_func=cofinanc.resample_bars,
        **node_resampling_config,
    )
    # Compute the node on the data.
    vwap_twap = node.fit(converted_data)
    # Save the result.
    vwap_twap_df = vwap_twap["df_out"]
    return vwap_twap_df


def calculate_returns(df: pd.DataFrame, rets_type: str) -> pd.DataFrame:
    """
    Compute returns on the resampled data DataFlow-style.

    :param df: Resampled multiindex DataFrame
    :param rets_type: i.e., "log_rets" or "pct_change"
    :return: The same DataFrame but with attached columns with returns
    """
    # Configure the node to calculate the returns.
    node_returns_config = {
        "in_col_groups": [
            ("close",),
            ("vwap",),
            ("twap",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "mode": rets_type,
        },
        "col_mapping": {
            "close": "close.ret_0",
            "vwap": "vwap.ret_0",
            "twap": "twap.ret_0",
        },
    }
    # Create the node that computes ret_0.
    nid = "ret0"
    node = dtfcore.GroupedColDfToDfTransformer(
        nid,
        transformer_func=cofinanc.compute_ret_0,
        **node_returns_config,
    )
    # Compute the node on the data.
    rets = node.fit(df)
    # Save the result.
    rets_df = rets["df_out"]
    return rets_df


def calculate_bid_ask_statistics(df: pd.DataFrame) -> pd.DataFrame:
    # Convert to multiindex.
    converted_df = dtfsysonod._convert_to_multiindex(df, "full_symbol")
    # Configure the node to calculate the returns.
    node_bid_ask_config = {
        "in_col_groups": [
            ("ask_price",),
            ("ask_size",),
            ("bid_price",),
            ("bid_size",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "bid_col": "bid_price",
            "ask_col": "ask_price",
            "bid_volume_col": "bid_size",
            "ask_volume_col": "ask_size",
        },
    }
    # Create the node that computes bid ask metrics.
    nid = "process_bid_ask"
    node = dtfcore.GroupedColDfToDfTransformer(
        nid,
        transformer_func=cfibiask.process_bid_ask,
        **node_bid_ask_config,
    )
    # Compute the node on the data.
    bid_ask_metrics = node.fit(converted_df)
    # Save the result.
    bid_ask_metrics = bid_ask_metrics["df_out"]
    # Convert relative spread to bps.
    bid_ask_metrics["relative_spread"] = (
        bid_ask_metrics["relative_spread"] * 10000
    )
    bid_ask_metrics = bid_ask_metrics.rename(
        columns={"relative_spread": "relative_spread_bps"}
    )
    return bid_ask_metrics


# TODO(gp): Move to hpandas.
def plot_without_gaps(df):
    # df.plot(x=df.index.astype(str))
    df = df.copy()
    df.index = df.index.to_series().dt.strftime("%Y-%m-%d")
    df.plot()
