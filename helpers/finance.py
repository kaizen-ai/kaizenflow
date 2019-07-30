import numpy as np

# TODO(gp): Extend to data frames.
def zscore(srs, com, demean, standardize, min_periods=None):
    # TODO(gp): Make sure timestamps are increasing.
    if min_periods is None:
        min_periods = 3 * com
    if demean:
        mean = srs.ewm(com=com, min_periods=min_periods).mean()
        srs = srs - mean
    if standardize:
        # TODO(gp): Remove nans, if needed.
        std = srs.ewm(com=com, min_periods=min_periods).std()
        srs = srs / std
    return srs


def show_distribution_by(by, ascending=False):
    by = by.sort_values(ascending=ascending)
    by.plot(kind="bar")


# #############################################################################

def annualize_sharpe_ratio(df_ret):
    # TODO(gp): Check that it's not increasing.
    return df_ret.mean() / df_ret.std() * np.sqrt(252)
