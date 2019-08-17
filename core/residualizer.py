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

import collections
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

    def __call__(self, obj, *args, **kwargs):
        if isinstance(obj, pd.Series):
            df = pd.DataFrame(obj)
        else:
            df = obj
        dbg.dassert_isinstance(df, pd.DataFrame)
        return self._execute(obj, *args, **kwargs)

    def _execute(self, df):
        raise NotImplementedError


class PcaFactorComputer(FactorComputer):

    def __init__(self, nan_mode_in_data, nan_mode_in_corr, sort_eigvals, stabilize_eig):
        super().__init__()
        self.nan_mode_in_data = nan_mode_in_data
        self.nan_mode_in_corr = nan_mode_in_corr
        self.sort_eigvals = sort_eigvals
        self.stabilize_eig = stabilize_eig
        self._eigval_df = collections.OrderedDict()
        self._eigvec_df = collections.OrderedDict()
        self._ts = []

    #def get_explained_variance(self):
    #    pass

    def get_eigenvalues(self):
        return self._eigval_df

    def get_eigenvalues(self):
        return self._eigvec_df

    @staticmethod
    def linearize_df(df):
        """
        Transform a pd.DataFrame:

                     0         1         2
            0  0.691443 -0.088121  0.717036
            1  0.656170 -0.338633 -0.674366
            2  0.302238  0.936783 -0.176323

        into a pd.Series

            f0_0    0.691443
            f0_1    0.656170
            f0_2    0.302238
            f1_0   -0.088121
            f1_1   -0.338633
            f1_2    0.936783
            f2_0    0.717036
            f2_1   -0.674366
            f2_2   -0.176323

        """
        df = df.copy()
        df.columns = ["f%s" % i for i in range(df.shape[1])]
        df.index = df.index.map(str)
        df = df.unstack()
        df.index = df.index.map('_'.join)
        return df

    def _execute(self, df, ts):
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
        self._eigval_df[ts] = eigval_df
        # Package and store eigenvectors.
        if np.isnan(eigval_df).all().all():
            eigvec = np.nan * eigvec
        # TODO(gp): Make sure eigenvec are normalized.
        eigvec_df = pd.DataFrame(eigvec, index=corr_df.columns)
        _LOG.debug("eigvec_df=%s", eigvec_df)
        if self.stabilize_eigvec:
            if self._ts:
                # Get previous ts.
                prev_ts = self._ts[-1]
                prev_eigvec_df = self._eigvec_df[prev_ts]
                col_map = self.stabilize_eigvec(prev_eigvec_df, eigvec_df)
                prev_eigval_df = self._eigval_df[prev_ts]
                # Use the col_map to reorg eigvec and eigval.
                #eigval_df = pd.Data
        # Get previous
        self._eigvec_df[ts] = eigvec_df
        self._ts.append(ts)
        # Package the results.
        #res = self.linearize(eigvec_df)
        #eigval_df
        #res.append()
        return eigvec_df

    @staticmethod
    def eigvec_distance(v1, v2):
        # TODO(gp): Maybe the max of the diff of the component is a better
        # metric.
        diff = np.linalg.norm(v1 - v2)
        _LOG.debug("v1=%s\nv2=%s\ndiff=%s", v1, v2, diff)
        return diff

    @staticmethod
    def stabilize_eigvec(prev_eigvec_df, eigvec_df):
        """
        Try to find a shuffling and sign changes of the columns in
        `prev_eigvec_df` to approximate `eigvec_df`.

        :return: map column index of original eigvec to (sign, column index
            of transformed eigvec)
        """

        def get_coeff(v1, v2, thr=1e-3):
            # TODO(gp): Use a loop to avoid repeatition.
            diff = PcaFactorComputer.eigvec_distance(v1, v2)
            _LOG.debug("v1=%s v2=%s -> diff=%s", v1, v2, diff)
            if diff < thr:
                return 1
            diff = PcaFactorComputer.eigvec_distance(v1, -v2)
            _LOG.debug("v1=%s v2=%s -> diff=%s", v1, v2, diff)
            if diff < thr:
                return -1
            return None

        # TODO(gp): This code can be sped up by:
        # 1) keeping a running list of the v2 columns already mapped so that
        #    we don't have to check over and over.
        # 2) once we find a match between columns, move to the next one v1
        # For now we just care about functionality.
        num_cols = eigvec_df.shape[1]
        col_map = {}
        for i in range(num_cols):
            for j in range(num_cols):
                coeff = get_coeff(prev_eigvec_df.iloc[:, i], eigvec_df.iloc[:, j])
                _LOG.debug("i=%s, j=%s, coeff=%s", i, j, coeff)
                if coeff:
                    _LOG.debug("i=%s -> j=%s", i, j)
                    dbg.dassert_not_in(i, col_map, msg="i=%s col_map=%s" % (
                     i, col_map))
                    col_map[i] = (coeff, j)
        return col_map

    @staticmethod
    def shuffle_eigval_eigvec(eigval_df, eigvec_df, col_map):
        """
        Transform the eigenvalues / eigenvectors according to a col_map
        returned by `stabilize_eigvec`.

        :return: updated eigvalues and eigenvectors
        """
        _LOG.debug("col_map=%s", col_map)
        # Apply the permutation to the eigenvalues / eigenvectors.
        permutation = [col_map[i][1] for i in sorted(col_map.keys())]
        _LOG.debug("permutation=%s", permutation)
        shuffled_eigvec_df = eigvec_df.reindex(columns=permutation)
        shuffled_eigvec_df.columns = range(shuffled_eigvec_df.shape[1])
        shuffled_eigval_df = eigval_df.reindex(index=permutation)
        shuffled_eigval_df.index = range(shuffled_eigval_df.shape[0])
        # Change the sign of the eigenvectors. We don't need to change the
        # sign of the eigenvalues.
        coeffs = pd.Series([col_map[i][0] for i in sorted(col_map.keys())])
        _LOG.debug("coeffs=%s", coeffs)
        shuffled_eigvec_df *= coeffs
        return shuffled_eigval_df, shuffled_eigvec_df

    @staticmethod
    def are_eigenvectors_stable(prev_eigvec_df, eigvec_df, thr=1e-3):
        """
        Return whether eigvec_df are "stable" in the sense that the change of
        corresponding of each eigenvec is smaller than a certain threshold.
        """
        dbg.dassert_eq(prev_eigvec_df.shape, eigvec_df.shape)
        num_fails = 0
        for i in range(eigvec_df.shape[1]):
            v1 = prev_eigvec_df.iloc[:, i]
            v2 = eigvec_df.iloc[:, i]
            _LOG.debug("v1=%s\nv2=%s", v1, v2)
            diff = PcaFactorComputer.eigvec_distance(v1, v2)
            if diff > thr:
                _LOG.debug("diff=%s > thr=%s -> num_fails=%d", diff, thr,
                           num_fails)
                num_fails += 1
        return num_fails

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



# We want to return linearized factors and coeff
# We need to unlinearize
# We can plot that directly
