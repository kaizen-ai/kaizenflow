"""
Import as:

import research_amp.transform as ramptran
"""


import matplotlib.pyplot as plt
import pandas as pd

import core.finance as cofinanc
import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod


def calculate_vwap_twap(df: pd.DataFrame, resampling_rule: str) -> pd.DataFrame:
    """
    Resample the data and calculate VWAP, TWAP using DataFlow methods.

    :param df: raw data
    :param resampling_rule: desired resampling frequency
    :return: resampled multiindex DataFrame with computed metrics
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

    :param df: resampled multiindex DataFrame
    :param rets_type: i.e., "log_rets" or "pct_change"
    :return: the same DataFrame but with attached columns with returns
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


def calculate_overtime_quantities(
    df_sample, full_symbol, resampling_rule, num_stds=1, plot_results=True
):
    # Choose specific `full_symbol`.
    data = df_sample.swaplevel(axis=1)[full_symbol]
    # Resample the data.
    resampler = cfinresa.resample(data, rule=resampling_rule)
    # Quoted spread.
    quoted_spread = resampler["quoted_spread"].mean()
    # Volatility of returns inside `buckets`.
    rets_vix = resampler["close.ret_0"].std().rename("rets_volatility")
    # Volume over time.
    volume = resampler["volume"].sum().rename("trading_volume")
    # Relative spread (in bps).
    rel_spread_bps = resampler["relative_spread_bps"].mean()
    # Bid / Ask value.
    bid_value = resampler["bid_value"].sum()
    ask_value = resampler["ask_value"].sum()
    # Tradability = abs(ret) / spread_bps.
    tradability = resampler["close.ret_0"].mean().abs() / rel_spread_bps
    tradability = tradability.rename("tradability")
    # Collect all the results.
    df = pd.concat(
        [
            quoted_spread,
            rets_vix,
            volume,
            rel_spread_bps,
            bid_value,
            ask_value,
            tradability,
        ],
        axis=1,
    )
    # Integrate time.
    df["time"] = df.index.time
    # Construct value curves over time.
    if plot_results:
        # Get rid of `time`.
        for cols in df.columns[:-1]:
            # Calculate man and std over the daytime.
            time_grouper = df.groupby("time")
            mean = time_grouper[cols].mean()
            std = time_grouper[cols].std()
            # Plot the results.
            fig = plt.figure()
            fig.suptitle(f"{cols} over time", fontsize=20)
            plt.ylabel(cols, fontsize=16)
            (mean + num_stds * std).plot(color="blue")
            mean.plot(lw=2, color="black")
    return df


def calculate_overtime_quantities_multiple_symbols(
    df_sample, full_symbols, resampling_rule, plot_results=True
):
    result = []
    # Calculate overtime stats for each `full_symbol`.
    for symb in full_symbols:
        df = calculate_overtime_quantities(
            df_sample, symb, resampling_rule, plot_results=False
        )
        df["full_symbol"] = symb
        result.append(df)
    mult_stats_df = pd.concat(result)
    # Convert to multiindex.
    mult_stats_df_conv = dtfsysonod._convert_to_multiindex(
        mult_stats_df, "full_symbol"
    )
    # Integrate time inside the day.
    mult_stats_df_conv["time_inside_days"] = mult_stats_df_conv.index.time
    # Compute the median value for all quantities.
    mult_stats_df_conv = mult_stats_df_conv.groupby("time_inside_days").agg(
        "median"
    )
    # Plot the results.
    if plot_results:
        # Get rid of `time` and `full_symbol`.
        for cols in mult_stats_df.columns[:-2]:
            mult_stats_df_conv[cols].plot(
                title=f"{cols} median over time", fontsize=12
            )
    return mult_stats_df_conv
