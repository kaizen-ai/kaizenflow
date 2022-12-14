"""
Import as:

import dataflow.model.metrics as dtfmodmetr
"""

import pandas as pd

import helpers.hdbg as hdbg


def convert_to_metrics_format(
    predict_df: pd.DataFrame,
    y_column_name: str,
    y_hat_column_name: str,
    *,
    asset_id_column_name: str = "asset_id"
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
    # Drop NaNs.
    metrics_df = predict_df.stack()
    condition = (metrics_df[y_column_name].isna()) | (
        metrics_df[y_hat_column_name].isna()
    )
    metrics_df = metrics_df[condition]
    #
    metrics_df.index.names = [metrics_df.index.names[0], asset_id_column_name]
    hdbg.dassert_eq(2, len(metrics_df.index.levels))
    return metrics_df
