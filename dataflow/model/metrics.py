"""
Import as:

import dataflow.model.metrics as dtfmodmetr
"""
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance.tradability as cfintrad
import core.statistics.requires_statsmodels as cstresta
import core.statistics.sharpe_ratio as cstshrat
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


# #############################################################################
# Data preprocessing
# #############################################################################


def _dassert_is_metrics_df(df: pd.DataFrame) -> None:
    """
    Check if the given df is a metrics_df.

    A metrics_df:
       - is indexed by the pair (end timestamp, asset id)
       - has all the features as columns
    """
    # Check that df is not empty.
    hdbg.dassert_lt(0, df.shape[0])
    # Check for number of index levels.
    idx_length = len(df.index.levels)
    hdbg.dassert_eq(idx_length, 2)
    hdbg.dassert_isinstance(df.columns, pd.Index)
    # We need to check both levels since timestamps of different assets have
    # an intersection, that means the 1st level is not unique among all assets.
    # However, it's unique for every asset separately.
    hpandas.dassert_strictly_increasing_index(df.index)
    # Check that the 1st level index is valid.
    dt_idx = df.index.get_level_values(0)
    hpandas.dassert_index_is_datetime(dt_idx)
    hdbg.dassert_eq(dt_idx.name, "end_ts")
    # Check that the 2nd level index is valid.
    asset_id_idx = df.index.get_level_values(1)
    hpandas.dassert_series_type_is(pd.Series(asset_id_idx), np.int64)
    hdbg.dassert_eq(asset_id_idx.name, "asset_id")


def convert_to_metrics_format(
    predict_df: pd.DataFrame,
    y_column_name: str,
    y_hat_column_name: str,
    *,
    asset_id_column_name: str = "asset_id",
) -> pd.DataFrame:
    """
    Transform a predict_df (i.e. output of DataFlow pipeline) into a
    metrics_df.

    A predict_df:
       - is indexed by the timestamp of the end of intervals
       - has multi-index column
       - the innermost column corresponds to assets (encoded as ints)
       - the outermost corresponding to "feature" (which can be inputs, outputs,
           and internal nodes)

    E.g.,
    ```
                              close                            ... close.ret_0
                              1030828978 1182743717 1464553467 ... 1030828978 1182743717 1464553467
    end_ts
    2022-08-31 20:00:00-04:00  NaN        NaN        NaN       ...  NaN        NaN        NaN
    2022-08-31 20:05:00-04:00  NaN        NaN        NaN       ...  NaN        NaN        NaN
    ...
    ```

    A metrics_df:
       - is indexed by the pair (end timestamp, asset id)
       - has all the features as columns

    E.g.,
    ```
                                            close        ...  close.ret_0
    end_ts                      asset_id
    2022-08-31 20:00:00-04:00   1030828978  0.6696       ...  NaN
                                1182743717  20016.4000   ...  NaN
                                1464553467  1551.9500    ...  NaN
    ...
    ```
    """
    # TODO(Grisha): add `_dassert_is_result_df()`.
    hdbg.dassert_eq(2, len(predict_df.columns.levels))
    _LOG.debug("predict_df=\n%s", hpandas.df_to_str(predict_df))
    # Drop NaNs.
    drop_kwargs = {
        "drop_infs": True,
        "report_stats": True,
        "subset": [y_column_name, y_hat_column_name],
    }
    metrics_df = predict_df.stack()
    metrics_df = hpandas.dropna(metrics_df, **drop_kwargs)
    #
    metrics_df.index.names = [metrics_df.index.names[0], asset_id_column_name]
    _dassert_is_metrics_df(metrics_df)
    _LOG.debug("metrics_df=\n%s", hpandas.df_to_str(metrics_df))
    return metrics_df


# #############################################################################
# Tags
# #############################################################################


# TODO(Grisha): @Dan Pass a list of tag modes instead of just 1.
def annotate_metrics_df(
    metrics_df: pd.DataFrame,
    tag_mode: str,
    *,
    tag_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute a tag (stored in `tag_col`) for each row of a `metrics_df` based on
    the requested `tag_mode`.

    The `tag_mode` is used to split the `metrics_df` in different chunks to
    compute metrics.

    :param tag_mode: symbolic name representing which criteria needs to be used
        to generate the tag
    :param tag_col: if None the standard name based on the `tag_mode` is used
    :return: `metrics_df` with a new column, e.g., if `tag_mode="hour"` a new column
        representing the number of hours is added
    """
    _dassert_is_metrics_df(metrics_df)
    _LOG.debug("metrics_df in=\n%s", hpandas.df_to_str(metrics_df))
    # Use the standard name based on `tag_mode`.
    if tag_col is None:
        tag_col = tag_mode
    # Check both index and columns as we cannot add a tag
    # that is an index already, e.g., `asset_id`.
    hdbg.dassert_not_in(tag_col, metrics_df.reset_index().columns)
    if tag_mode == "hour":
        idx_datetime = metrics_df.index.get_level_values(0)
        metrics_df[tag_col] = idx_datetime.hour
    elif tag_mode == "all":
        metrics_df[tag_col] = tag_mode
    elif tag_mode == "magnitude_quantile_rank":
        # Get the asset id index name to group data by.
        idx_name = metrics_df.index.names[1]
        # TODO(Nina): Pass target column name and number of quantiles via config.
        qcut_func = lambda x: pd.qcut(x, 10, labels=False)
        magnitude_quantile_rank = metrics_df.groupby(idx_name)[
            "vwap.ret_0.vol_adj"
        ].transform(qcut_func)
        metrics_df[tag_col] = magnitude_quantile_rank
    else:
        raise ValueError(f"Invalid tag_mode={tag_mode}")
    _LOG.debug("metrics_df out=\n%s", hpandas.df_to_str(metrics_df))
    return metrics_df


# #############################################################################
# Metrics
# #############################################################################


# TODO(Grisha): double check the return type, i.e. 1 and -1,
# it seems working well with `calculate_hit_rate()` but is
# counter-intuitive. 
# TODO(Grisha): move to `core/finance.py`.
def compute_hit(
    y: pd.Series,
    y_hat: pd.Series,
) -> pd.Series:
    """
    Compute hit.

    Hit is 1 when prediction's sign matches target variable's sign,
    otherwise it is -1.
    The function returns -1 and 1 for compatibility with `calculate_hit_rate()`.

    :param y: target variable
    :param y_hat: predicted value of y
    :return: hit for each pair of (y, y_hat)
    """
    hdbg.dassert_isinstance(y, pd.Series)
    hdbg.dassert_lt(0, y.shape[0])
    hdbg.dassert_isinstance(y_hat, pd.Series)
    hdbg.dassert_lt(0, y_hat.shape[0])
    # Compute hit and convert boolean values to 1 and -1.
    hit = ((y * y_hat) >= 0).astype(int)
    hit[hit == 0] = -1
    return hit


def apply_metrics(
    metrics_df: pd.DataFrame,
    tag_col: str,
    metric_modes: List[str],
    config: cconfig.Config,
) -> pd.DataFrame:
    """
    Given a metric_dfs tagged with `tag_col`, compute the metrics corresponding
    to `metric_modes`.

    E.g., `using tag_col = "asset_id"` and `metric_modes=["pnl", "hit_rate"]` the
    output is like:
    ```
                hit_rate   pnl
    asset_id
    1030828978  0.327243   0.085995
    1182743717  0.330533   0.045795
    1464553467  0.328712   0.095809
    1467591036  0.331297   0.059772
    ...
    ```

    :param metrics_df: metrics_df annotated with tag
    :param tag_col: the column to be used to split the metrics_df
    :param metric_modes: a list of strings representing the metrics to compute
        (e.g., hit rate, pnl)
    :param config: config that controls metrics parameters
    :return: the result is a df that has:
        - as index the values of the tags
        - as columns the names of the applied metrics
    """
    _dassert_is_metrics_df(metrics_df)
    _LOG.debug("metrics_df in=\n%s", hpandas.df_to_str(metrics_df))
    # Check both index and columns, e.g., `asset_id` is an index but
    # we still can group by it.
    hdbg.dassert_in(tag_col, metrics_df.reset_index().columns)
    #
    y_column_name = config["y_column_name"]
    y_hat_column_name = config["y_hat_column_name"]
    hit_col_name = config["hit_col_name"]
    bar_pnl_col_name = config["bar_pnl_col_name"]
    #
    y = metrics_df[y_column_name]
    y_hat = metrics_df[y_hat_column_name]
    #
    out_dfs = []
    for metric_mode in metric_modes:
        if metric_mode == "hit_rate":
            if hit_col_name not in metrics_df.columns:
                # Compute hit.
                metrics_df[hit_col_name] = compute_hit(y, y_hat)
            # Compute hit rate per tag column.
            group_df = metrics_df.groupby(tag_col)
            srs = group_df[hit_col_name].apply(
                lambda x: cstresta.calculate_hit_rate(
                    x, **config["calculate_hit_rate_kwargs"]
                )
            )
            df_tmp = srs.to_frame()
            # Set output to the desired format.
            df_tmp = df_tmp.unstack(level=1)
            df_tmp.columns = df_tmp.columns.droplevel(0)
        elif metric_mode == "pnl":
            if bar_pnl_col_name not in metrics_df.columns:
                # Compute bar PnL.
                metrics_df[bar_pnl_col_name] = cfintrad.compute_bar_pnl(
                    metrics_df, y_column_name, y_hat_column_name
                )
            # Compute bar PnL per tag column.
            group_df = metrics_df.groupby(tag_col)
            srs = group_df.apply(
                lambda x: cfintrad.compute_total_pnl(
                    x, y_column_name, y_hat_column_name
                )
            )
            # TODO(Grisha): ideally we should pass it via config too, same below.
            srs.name = "total_pnl"
            df_tmp = srs.to_frame()
        elif metric_mode == "sharpe_ratio":
            # We need computed PnL to compute Sharpe ratio.
            if bar_pnl_col_name not in metrics_df.columns:
                # Compute bar PnL.
                metrics_df[bar_pnl_col_name] = cfintrad.compute_bar_pnl(
                    metrics_df, y_column_name, y_hat_column_name
                )
            time_scaling = config["time_scaling"]
            # Compute Sharpe ratio per tag column.
            group_df = metrics_df.groupby(tag_col)
            srs = group_df[bar_pnl_col_name].apply(
                lambda x: cstshrat.compute_sharpe_ratio(x, time_scaling)
            )
            srs.name = "SR"
            df_tmp = srs.to_frame()
        else:
            raise ValueError(f"Invalid metric_mode={metric_mode}")
        out_dfs.append(df_tmp)
    out_df = pd.concat(out_dfs, axis=1)
    _LOG.debug("metrics_df out=\n%s", hpandas.df_to_str(out_df))
    return out_df
