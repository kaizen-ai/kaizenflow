import numpy as np
import pandas as pd

import helpers.dbg as dbg


def zscore(obj, com, demean, standardize, delay, min_periods=None):
    dbg.dassert_type_in(obj, (pd.Series, pd.DataFrame))
    # z-scoring might not be causal with delay=0, especially for predicted
    # variables.
    dbg.dassert_lte(0, delay)
    # Make sure timestamps are increasing.
    dbg.dassert(obj.index.is_monotonic)
    obj = obj.copy()
    if min_periods is None:
        min_periods = 3 * com
    if demean:
        mean = obj.ewm(com=com, min_periods=min_periods).mean()
        if delay != 0:
            mean = mean.shift(delay)
        obj = obj - mean
    if standardize:
        # TODO(gp): Remove nans, if needed.
        std = obj.ewm(com=com, min_periods=min_periods).std()
        if delay != 0:
            std = std.shift(delay)
        obj = obj / std
    return obj


def show_distribution_by(by, ascending=False):
    by = by.sort_values(ascending=ascending)
    by.plot(kind="bar")


# #############################################################################


def annualize_sharpe_ratio(df_ret):
    # TODO(gp): Check that it's not increasing.
    return df_ret.mean() / df_ret.std() * np.sqrt(252)
