import logging
from typing import Iterable, List, Optional, Tuple

import gluonts
import pandas as pd

import helpers.dbg as dbg

# TODO(*): gluon needs these two imports to work properly.
import gluonts.dataset.common as gdc  # isort:skip # noqa: F401 # pylint: disable=unused-import
import gluonts.dataset.util as gdu  # isort:skip # noqa: F401 # pylint: disable=unused-import


_LOG = logging.getLogger(__name__)


def transform_pandas_gluon(
    df: pd.DataFrame, frequency: str, x_vars: Iterable[str], y_vars: Iterable[str]
) -> gluonts.dataset.common.ListDataset:
    """
    Transform pd.DataFrame into gluonts `ListDataset`.

    :param df: dataframe with feature and target columns
    :param frequency: pandas frequency
    :param x_vars: names of feature columns
    :param y_vars: names of target columns
    :return: gluonts `ListDataset`
    """
    if isinstance(y_vars, str):
        y_vars = [y_vars]
    dbg.dassert_is_subset(x_vars, df.columns)
    dbg.dassert_is_subset(y_vars, df.columns)
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    dbg.dassert_monotonic_index(df)
    df = df.asfreq(frequency)
    ts = gluonts.dataset.common.ListDataset(
        [
            {
                gluonts.dataset.field_names.FieldName.TARGET: df[y_var],
                gluonts.dataset.field_names.FieldName.START: df.index[0],
                gluonts.dataset.field_names.FieldName.FEAT_DYNAMIC_REAL: df[
                    x_vars
                ],
            }
            for y_var in y_vars
        ],
        freq=frequency,
    )
    return ts


def transform_gluon_pandas(
    gluon_ts: gluonts.dataset.common.ListDataset,
    x_vars: Iterable[str],
    y_vars: Iterable[str],
    index_name: Optional[str],
) -> List[Tuple[pd.DataFrame, pd.Series]]:
    """
    Transform gluonts `ListDataset` into target and feature pd.DataFrames.

    :param gluon_ts: gluonts `ListDataset`
    :param x_vars: names of feature columns
    :param y_vars: names of target columns
    :param index_name: name of the index
    :return: [(features, target)]
    """
    if isinstance(y_vars, str):
        y_vars = [y_vars]
    dbg.dassert_eq(
        len(y_vars),
        len(gluon_ts),
        "The lenghts of `y_vars` and `gluon_ts` should be equal",
    )
    dfs = []
    for ts, y_var in zip(iter(gluon_ts), y_vars):
        target = gluonts.dataset.util.to_pandas(ts)
        target.name = y_var
        features = pd.DataFrame(
            ts[gluonts.dataset.field_names.FieldName.FEAT_DYNAMIC_REAL],
            index=target.index,
        )
        features.columns = x_vars
        features.index.name = index_name
        dfs.append((features, target))
    return dfs
