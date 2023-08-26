"""
Import as:

import dataflow_amp.system.Cx.utils as dtfasycxut
"""

import logging

import pandas as pd

import core.finance as cofinanc
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import market_data as mdata

_LOG = logging.getLogger(__name__)


# TODO(Grisha): consider moving to `dataflow`, maybe `dataflow/core/utils.py`,
# there is nothing specific to Cx.
# #############################################################################
# Load and resample OHLCV data
# #############################################################################


def load_ohlcv_data(
    market_data: mdata.MarketData,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Load OHLCV data using OHLCV `MarketData`.

    :param market_data: `MarketData` backed by the production DB
    :param start_timestamp: the earliest date timestamp to load data for
    :param end_timestamp: the latest date timestamp to load data for
    :return: load OHLCV dataframe
    """
    nid = "read_data"
    timestamp_col_name = "end_timestamp"
    multiindex_output = True
    col_names_to_remove = None
    # This is similar to what `RealTimeDataSource` does in production
    # but allows to query data in the past.
    historical_data_source = dtfsys.HistoricalDataSource(
        nid,
        market_data,
        timestamp_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Convert to the DataFlow `Intervals` format.
    fit_intervals = [(start_timestamp, end_timestamp)]
    _LOG.info("fit_intervals=%s", fit_intervals)
    historical_data_source.set_fit_intervals(fit_intervals)
    df_ohlcv = historical_data_source.fit()["df_out"]
    return df_ohlcv


def resample_ohlcv_data(
    ohlcv_df: pd.DataFrame,
    resampling_frequency: str,
) -> pd.DataFrame:
    """
    Resample OHLCV to a target frequency.

    :param ohlcv_df: OHLCV dataframe
    :param resampling_frequency: resampling frequency
    :return: resampled OHLCV dataframe
    """
    resampling_node = dtfcore.GroupedColDfToDfTransformer(
        "resample",
        transformer_func=cofinanc.resample_bars,
        **{
            "in_col_groups": [
                ("open",),
                ("high",),
                ("low",),
                ("close",),
                ("volume",),
            ],
            "out_col_group": (),
            "transformer_kwargs": {
                "rule": resampling_frequency,
                "resampling_groups": [
                    ({"close": "close"}, "last", {}),
                    ({"high": "high"}, "max", {}),
                    ({"low": "low"}, "min", {}),
                    ({"open": "open"}, "first", {}),
                    (
                        {"volume": "volume"},
                        "sum",
                        {"min_count": 1},
                    ),
                    (
                        {
                            "close": "twap",
                        },
                        "mean",
                        {},
                    ),
                ],
                "vwap_groups": [
                    ("close", "volume", "vwap"),
                ],
            },
            "reindex_like_input": False,
            "join_output_with_input": False,
        },
    )
    resampled_ohlcv = resampling_node.fit(ohlcv_df)["df_out"]
    return resampled_ohlcv


def load_and_resample_ohlcv_data(
    market_data: mdata.MarketData,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    bar_duration: str,
) -> pd.DataFrame:
    """
    Load OHLCV data and resample it.

    :param market_data: same as in `load_ohlcv_data()`
    :param start_timestamp: same as in `load_ohlcv_data()`
    :param end_timestamp: same as in `load_ohlcv_data()`
    :param bar_duration: bar duration as pandas string
    :return: resampled OHLCV dataframe
    """
    ohlcv_data = load_ohlcv_data(market_data, start_timestamp, end_timestamp)
    resampled_ohlcv = resample_ohlcv_data(ohlcv_data, bar_duration)
    return resampled_ohlcv
