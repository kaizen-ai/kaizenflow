"""
Import as:

import core.data_adapters as adpt
"""

import logging
from typing import Dict, Generator, Iterable, List, Optional, Tuple, Union

import gluonts
import pandas as pd

import helpers.dbg as dbg

# TODO(*): gluon needs these two imports to work properly.
import gluonts.dataset.common as gdc  # isort:skip # noqa: F401 # pylint: disable=unused-import


_LOG = logging.getLogger(__name__)


def _iter_multiindex(
    local_ts: pd.DataFrame,
    frequency: str,
    x_vars: Iterable[str],
    y_vars: Iterable[str],
) -> Generator[Dict[str, Union[pd.DataFrame, pd.Timestamp]], None, None]:
    """
    Iterate level 0 of MultiIndex and generate `data_iter` parameter for
    `gluonts.dataset.common.ListDatase`.
    """
    # Insert a level if the dataframe does not have a MultiIndex.
    if not isinstance(local_ts.index, pd.MultiIndex):
        local_ts = pd.concat([local_ts], keys=[0])
    for _, local_ts_grid in local_ts.groupby(level=0):
        df = local_ts_grid.droplevel(0)
        dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        dbg.dassert_monotonic_index(df)
        df = df.asfreq(frequency)
        yield {
            gluonts.dataset.field_names.FieldName.TARGET: df[y_vars],
            gluonts.dataset.field_names.FieldName.START: df.index[0],
            gluonts.dataset.field_names.FieldName.FEAT_DYNAMIC_REAL: df[x_vars],
        }


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
    ts = gluonts.dataset.common.ListDataset(
        _iter_multiindex(df, frequency, x_vars, y_vars),
        freq=frequency,
        one_dim_target=False,
    )
    return ts


def transform_gluon_pandas(
    gluon_ts: gluonts.dataset.common.ListDataset,
    x_vars: Iterable[str],
    y_vars: Iterable[str],
    index_name: Optional[str],
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
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
    dfs = []
    for ts in iter(gluon_ts):
        target = pd.DataFrame(ts[gluonts.dataset.field_names.FieldName.TARGET])
        idx = pd.date_range(
            ts[gluonts.dataset.field_names.FieldName.START],
            periods=target.shape[0],
            freq=ts[gluonts.dataset.field_names.FieldName.START].freq,
        )
        target.index = idx
        target.columns = y_vars
        features = pd.DataFrame(
            ts[gluonts.dataset.field_names.FieldName.FEAT_DYNAMIC_REAL],
            index=idx,
        )
        features.columns = x_vars
        features.index.name = index_name
        dfs.append((features, target))
    return dfs


def _convert_tuples_list_to_multiindex_df(
    dfs: List[Tuple[pd.DataFrame, pd.DataFrame]]
) -> pd.DataFrame:
    dfs = [pd.concat([features, target], axis=1) for features, target in dfs]
    return pd.concat(dfs, keys=range(len(dfs)))


def transform_gluon_multiindex_pandas(
    gluon_ts: gluonts.dataset.common.ListDataset,
    x_vars: Iterable[str],
    y_vars: Iterable[str],
    index_name: Optional[str],
) -> pd.DataFrame:
    """
    Transform gluonts `ListDataset` into multiindexed dataframe.

    :param gluon_ts: gluonts `ListDataset`
    :param x_vars: names of feature columns
    :param y_vars: names of target columns
    :param index_name: name of the index
    :return: multiindexed dataframe
    """
    dfs = transform_gluon_pandas(gluon_ts, x_vars, y_vars, index_name)
    return _convert_tuples_list_to_multiindex_df(dfs)
