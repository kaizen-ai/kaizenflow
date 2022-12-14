"""
Import as:

import dataflow.model.metrics as dtfmodmetr
"""
import logging
from typing import Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


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
    metrics_df = predict_df.stack()
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

    The `tag_mode` is used to split the `metrics_df` in different chunks to compute metrics.

    :param tag_mode: symbolic name representing which criteria needs to be used to generate the tag
    :param tag_col: if None the standard name based on the `tag_mode` is used
    :return: `metrics_df` with a new column, e.g., if `tag_mode="hour"` a new column
        representing the number of hours is added
    """
    _LOG.debug("metrics_df=\n%s", hpandas.df_to_str(metrics_df))
    # Create a copy in order not to modify the input.
    metrics_df_copy = metrics_df.copy()
    # Use the standard name based on `tag_mode`.
    if tag_col is None:
        tag_col = tag_mode
    hdbg.dassert_not_in(tag_col, metrics_df.columns)
    if tag_mode == "hour":
        metrics_df_copy[tag_col] = metrics_df_copy.index.get_level_values(0).hour
    else:
        raise ValueError(f"Invalid tag_mode={tag_mode}")
    _LOG.debug("metrics_df_copy=\n%s", hpandas.df_to_str(metrics_df_copy))
    return metrics_df_copy
