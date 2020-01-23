"""
Import as:

import core.data_adapters as adpt
"""

import logging
from typing import Callable, List, Tuple, Union

import pandas as pd

_LOG = logging.getLogger(__name__)


def transform_to_sklearn(
    df: pd.DataFrame, x_vars: List[str], y_vars: List[str]
) -> Tuple[pd.Index, List[str], pd.DataFrame, List[str], pd.DataFrame]:
    """
    Transform pd.DataFrame into sklearn model inputs.

    :param df: input dataset
    :param x_vars: names of feature columns
    :param y_vars: names of target columns
    :return: (index, x_vars, x_vals, y_vars, y_vals)
    """
    idx = df.index
    df = df.reset_index()
    x_vars = _to_list(x_vars)
    y_vars = _to_list(y_vars)
    x_vals = df[x_vars]
    y_vals = df[y_vars]
    return idx, x_vars, x_vals, y_vars, y_vals


def transform_from_sklearn(
    idx: pd.Index,
    x_vars: List[str],
    x_vals: pd.DataFrame,
    y_vars: List[str],
    y_vals: pd.DataFrame,
    y_hat: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transform data from sklearn format to pandas dataframes.

    :param idx: data index
    :param x_vars: names of feature columns
    :param x_vals: features data
    :param y_vars: names of target columns
    :param y_vals: targets data
    :param y_hat: predictions
    :return: dataframes of features, targets and predictions
    """
    x = pd.DataFrame(x_vals.values, index=idx, columns=x_vars)
    y = pd.DataFrame(y_vals.values, index=idx, columns=y_vars)
    y_h = pd.DataFrame(y_hat, index=idx, columns=[y + "_hat" for y in y_vars])
    return x, y, y_h


def _to_list(to_list: Union[List[str], Callable[[], List[str]]]) -> List[str]:
    """
    Return a list given its input.

    - If the input is a list, the output is the same list.
    - If the input is a function that returns a list, then the output of
      the function is returned.

    How this might arise in practice:
      - A ColumnTransformer returns a number of x variables, with the
        number dependent upon a hyperparameter expressed in config
      - The column names of the x variables may be derived from the input
        dataframe column names, not necessarily known until graph execution
        (and not at construction)
      - The ColumnTransformer output columns are merged with its input
        columns (e.g., x vars and y vars are in the same DataFrame)
    Post-merge, we need a way to distinguish the x vars and y vars.
    Allowing a callable here allows us to pass in the ColumnTransformer's
    method `transformed_col_names` and defer the call until graph
    execution.
    """
    if callable(to_list):
        to_list = to_list()
    if isinstance(to_list, list):
        return to_list
    raise TypeError("Data type=`%s`" % type(to_list))
