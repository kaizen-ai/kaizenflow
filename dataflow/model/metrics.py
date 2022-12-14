"""
Import as:

import dataflow.model.metrics as dtfmodmetr
"""

import pandas as pd

import helpers.hdbg as hdbg


def convert_to_metrics_format(
    predict_df: pd.DataFrame, *, asset_id_column_name: str = "asset_id"
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

    # TODO(Grisha): @Nina add a snippet.
    E.g.,
    ```
    ...
    ```

    A metrics_df:
       - is indexed by the pair (end timestamp, asset id)
       - has all the features as columns

    # TODO(Grisha): @Nina add a snippet.
    E.g.,
    ```
    ...
    ```
    """
    hdbg.dassert_eq(2, len(predict_df.columns.levels))
    # TODO(Grisha): @Nina drop rows where y or y_hat is NaN.
    metrics_df = predict_df.stack()
    metrics_df.index.names = [metrics_df.index.names[0], asset_id_column_name]
    hdbg.dassert_eq(2, len(metrics_df.index.levels))
    return metrics_df
