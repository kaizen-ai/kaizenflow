"""
Import as:

import dataflow.model.metrics as dtfmodmetr
"""
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)



def _dassert_is_metrics_df(df: pd.DataFrame) -> None:
    """
    Check if the given df is a metrics_df.

    A metrics_df:
       - has a multi-index
       - is indexed by the timestamp of the end of intervals and asset ids
    """
    # Check that df is not empty.
    hdbg.dassert_lt(1, df.shape[0])
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
    hdbg.dassert_eq(2, len(metrics_df.index.levels))
    _LOG.debug("metrics_df=\n%s", hpandas.df_to_str(metrics_df))
    return metrics_df


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
    hdbg.dassert_not_in(tag_col, metrics_df.reset_index().columns)
    if tag_mode == "hour":
        idx = metrics_df.index.get_level_values(0)
        metrics_df[tag_col] = idx.hour
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


def compute_hit(
    y: pd.Series,
    y_hat: pd.Series,
) -> pd.Series:
    """
    Compute hit.

    Hit is `True` when prediction's sign matches target variable's sign,
    otherwise it is `False`.

    :param y: target variable
    :param y_hat: predicted value of y
    :return: hit for each pair of (y, y_hat)
    """
    hdbg.dassert_isinstance(y, pd.Series)
    hdbg.dassert_lt(0, y.shape[0])
    hdbg.dassert_isinstance(y_hat, pd.Series)
    hdbg.dassert_lt(0, y_hat.shape[0])
    #
    hit = (y * y_hat) >= 0
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

    E.g., `using tag_col = "asset_id"` and `metric_mode=["pnl", "hit_rate"]` the
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
    hdbg.dassert_in(tag_col, metrics_df.columns)
    #
    y = metrics_df[config["y_column_name"]]
    y_hat = metrics_df[config["y_hat_column_name"]]
    #
    out_dfs = []
    for metric_mode in metric_modes:
        if metric_mode == "hit_rate":
            metrics_df[metric_mode] = compute_hit(y, y_hat)
            # TODO(Grisha): add CIs and re-use `calculate_hit_rate()`.
            srs = metrics_df.groupby(tag_col)[metric_mode].agg(np.mean)
        else:
            raise ValueError(f"Invalid metric_mode={metric_mode}")
        df_tmp = srs.to_frame()
        out_dfs.append(df_tmp)
    out_df = pd.concat(out_dfs)
    _LOG.debug("metrics_df out=\n%s", hpandas.df_to_str(out_df))
    return out_df
