import numpy as np


def annualize_sharpe_ratio(df_ret):
    # TODO(gp): Check that it's not increasing.
    return df_ret.mean() / df_ret.std() * np.sqrt(252)


def zscore(rets, n_win=28):
    """
    zscore by rolling std dev.
    """
    zrets = ((rets - rets.rolling(n_win, min_periods=n_win).mean()) /
             rets.rolling(n_win, min_periods=n_win).std())
    return zrets


def show_distribution_by(by, ascending=False):
    by = by.sort_values(ascending=ascending)
    by.plot(kind="bar")
