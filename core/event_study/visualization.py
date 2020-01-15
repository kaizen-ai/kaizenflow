"""
Import as:

import core.event_study.visualization as esvis
"""
import logging
from typing import Any, Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import core.signal_processing as sigp
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def plot_event_response(local_ts: pd.DataFrame, response_col: str) -> None:
    """
    Plot lineplot of event response curve.

    :param local_ts: output of `core.event_study.core.build_local_timeseries`
    :param response_col: name of response column
    """
    local_ts = local_ts.copy()
    local_ts.index.names = ["grid_index", "event_timestamp"]
    local_ts = local_ts.reset_index()
    sns.lineplot(x="grid_index", y=response_col, data=local_ts)
    _ = plt.xticks(local_ts["grid_index"].unique())


def plot_inter_event_time_gaps(
    timestamps: pd.Series,
    time_unit: str,
    lower_quantile: float = 0.01,
    mode: Optional[str] = "set_to_nan",
    **kwargs: Any,
) -> None:
    """
    Plot inter-event time gaps.

    :param timestamps: datetime pd.Series
    :param time_unit: time units in which to plot the data. A parameter
        for timedelta64[]
    :param lower_quantile: as in sigp.process_outliers
    :param mode: as in sigp.process_outliers
    :param kwargs: passed into sigp.process_outliers
    """
    dbg.dassert_isinstance(
        timestamps[0], pd.Timestamp, "The data should be datetime"
    )
    time_gaps = timestamps.diff()
    time_gaps_wo_outliers = sigp.process_outliers(
        time_gaps.astype(f"timedelta64[{time_unit}]"),
        mode,
        lower_quantile,
        **kwargs,
    ).dropna()
    sns.distplot(time_gaps_wo_outliers)
    plt.xlabel(time_unit)
    plt.title("Distribution of inter-event time gaps without outliers")
