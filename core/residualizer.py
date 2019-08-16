"""
Implement a residualizer pipeline that, given data (e.g., returns or features),
computes:
1) factors
2) loadings
3) residuals

Each of the 3 components:
- has an interface and multiple implementations
- supports both stateful and stateless operations
- supports a functional style, composable with pandas and following some pandas
  conventions
- can be used in sklearn Pipeline objects
"""

import logging

import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.explore as exp

_LOG = logging.getLogger(__name__)


# TODO(gp): Make sure it's sklearn complaint
# TODO(gp): We can use abstract classes which now are better supported in
# python3 (see https://docs.python.org/3/library/abc.html). Although abstract
# classes is not as important, with duck typing.
class FactorComputer:

    def __init__(self):
        pass

    def fit(self):
        # TODO(gp): Implement sklearn complaint method.
        raise NotImplementedError

    def transform(self):
        # TODO(gp): Implement sklearn complaint method.
        raise NotImplementedError

    def get_factors(self):
        raise NotImplementedError

    #def get_loading(self):
    #    raise NotImplementedError

    def __call__(self, obj):
        if isinstance(obj, pd.Series):
            df = pd.DataFrame(obj)
        else:
            df = obj
        dbg.dassert_isinstance(df, pd.DataFrame)
        return self._execute(df)

    def _execute(self, df):
        raise NotImplementedError


class PcaFactorComputer(FactorComputer):

    def __init__(self, nan_mode_in_data, nan_mode_in_corr, sort_eigvals):
        super().__init__()
        self.nan_mode_in_data = nan_mode_in_data
        self.nan_mode_in_corr = nan_mode_in_corr
        self.sort_eigvals = sort_eigvals
        self._eigval_df = []
        self._eigvec_df = []

    #def get_explained_variance(self):
    #    pass

    def get_eigenvalues(self):
        return self._eigval_df

    def get_eigenvalues(self):
        return self._eigvec_df

    def _execute(self, df):
        dbg.check_monotonic_df(df)
        # Compute correlation.
        df = exp.handle_nans(df, self.nan_mode_in_data)
        corr_df = df.corr()
        corr_df = exp.handle_nans(corr_df, self.nan_mode_in_corr)
        _LOG.debug("corr_df=%s", corr_df)
        # Use the last datetime as timestamp.
        dt = df.index.max()
        _LOG.debug("ts=%s", dt)
        # Compute eigenvalues and eigenvectors.
        eigval, eigvec = np.linalg.eig(corr_df)
        # Sort eigenvalues, if needed.
        if not (sorted(eigval) == eigval).all():
            _LOG.debug("eigvals not sorted: %s", eigval)
            if self.sort_eigvals:
                _LOG.debug("Before sorting:\neigval=\n%s\neigvec=\n%s", eigval,
                           eigvec)
                idx = eigval.argsort()[::-1]
                eigval = eigval[idx]
                eigvec = eigvec[:, idx]
        _LOG.debug("eigval=\n%s\neigvec=\n%s", eigval, eigvec)
        # Package and store eigenvalues.
        eigval_df = pd.DataFrame([eigval], index=[dt])
        eigval_df = eigval_df.multiply(1 / eigval_df.sum(axis=1), axis="index")
        _LOG.debug("eigval_df=%s", eigval_df)
        self._eigval_df.append(eigval_df)
        # Package and store eigenvectors.
        if np.isnan(eigval_df).all().all():
            eigvec = np.nan * eigvec
        # TODO(gp): Make sure eigenvec are normalized.
        eigvec_df = pd.DataFrame(eigvec, index=corr_df.columns)
        # Add another index.
        eigvec_df.index.name = ""
        eigvec_df.reset_index(inplace=True)
        eigvec_df.insert(0, 'datetime', dt)
        eigvec_df.set_index(["datetime", ""], inplace=True)
        _LOG.debug("eigvec_df=%s", eigvec_df)
        self._eigvec_df.append(eigvec_df)
        return eigvec_df

    def plot_over_time(self, num_pcs_to_plot=0, num_cols=2):
        """
        Similar to plot_pca_analysis() but over time.
        """
        # Plot eigenvalues.
        self._eigval_df.plot(title='Eigenvalues over time', ylim=(0, 1))
        # Plot cumulative variance.
        self._eigval_df.cumsum(axis=1).plot(
            title='Fraction of variance explained by top PCs over time',
            ylim=(0, 1))
        # Plot eigenvectors.
        max_pcs = self._eigvec_df.shape[1]
        num_pcs_to_plot = self._get_num_pcs_to_plot(num_pcs_to_plot, max_pcs)
        _LOG.info("num_pcs_to_plot=%s", num_pcs_to_plot)
        if num_pcs_to_plot > 0:
            _, axes = _get_multiple_plots(
                num_pcs_to_plot,
                num_cols=num_cols,
                y_scale=4,
                sharex=True,
                sharey=True)
            for i in range(num_pcs_to_plot):
                self._eigvec_df[i].unstack(1).plot(
                    ax=axes[i], ylim=(-1, 1), title='PC%s' % i)

    @staticmethod
    def _get_num_pcs_to_plot(num_pcs_to_plot, max_pcs):
        """
        Get the number of principal components to plot.
        """
        if num_pcs_to_plot == -1:
            num_pcs_to_plot = max_pcs
        dbg.dassert_lte(0, num_pcs_to_plot)
        dbg.dassert_lte(num_pcs_to_plot, max_pcs)
        return num_pcs_to_plot

## NOTE:
##   - DRY: We have a rolling corr function elsewhere.
##   - Functional style: This one seems to be able to modify `ret` through
##     `nan_mode`.
#def rolling_corr_over_time(df, com, nan_mode):
#    """
#    Compute rolling correlation over time.
#    :return: corr_df is a multi-index df storing correlation matrices with
#        labels
#    """
#    dbg.check_monotonic_df(df)
#    df = handle_nans(df, nan_mode)
#    #corr_df = df.ewm(com=com, min_periods=3 * com).corr()
#    corr_df = df.rolling(com).corr()
#    return corr_df
#
#
#df2 = df.rolling(com=..., window).corr()
#
#
#def corr_tmp():
#    sklearn....
#    #
#    PCA
#
#
#df2 = df.rolling(com=..., window).apply(corr_tmp)
#
#
#
#res = PcaResidualizer(...)
#
#df2 = df.rolling(com=..., window).apply(res)
#
#df = date   factors     loadings
#
#res.get_...()
#
#
