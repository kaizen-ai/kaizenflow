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


# TODO(gp): This is probably general and should be moved somewhere else.
def linearize_df(df, prefix):
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
    df.columns = ["%s%s" % (prefix, i) for i in range(df.shape[1])]
    df.index = df.index.map(str)
    df = df.unstack()
    df.index = df.index.map('_'.join)
    return df


# ##############################################################################


# TODO(gp): Make sure it's sklearn complaint
# TODO(gp): Use abstract classes (see https://docs.python.org/3/library/abc.html).
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

    def __call__(self, obj, *args, **kwargs):
        if isinstance(obj, pd.Series):
            df = pd.DataFrame(obj)
        else:
            df = obj
        dbg.dassert_isinstance(df, pd.DataFrame)
        return self._execute(obj, *args, **kwargs)

    def _execute(self, df, ts):
        raise NotImplementedError


# ##############################################################################


from scipy.spatial.distance import cosine

# TODO(gp): eigval_df -> eigval since it's a Series?
# eigvec_df -> eigvec
class PcaFactorComputer(FactorComputer):
    """
    Compute factors using a rolling PCA decomposition.
    """

    def __init__(self, nan_mode_in_data, nan_mode_in_corr, sort_eigvals,
                 stabilize_eig):
        """

        :param nan_mode_in_data: how to handle NAs in data passed for processing
            (see `handle_nans()`)
        :param nan_mode_in_corr: how to handle NAs in correlation matrix
        :param sort_eigvals: force sorting of the eigenvalues
        :param stabilize_eig: stabilize eigenvalues / eigenvectors by
            reordering them and changing sign
        """
        super().__init__()
        self.nan_mode_in_data = nan_mode_in_data
        self.nan_mode_in_corr = nan_mode_in_corr
        self.sort_eigvals = sort_eigvals
        self.stabilize_eig = stabilize_eig
        # Map from timestamps to eigval / eigvec.
        self._ts = []
        self._eigval_df = collections.OrderedDict()
        self._eigvec_df = collections.OrderedDict()

    #def get_explained_variance(self):
    #    pass

    def get_eigenvalues(self):
        return self._eigval_df

    def get_eigenvectors(self):
        return self._eigvec_df

    @staticmethod
    def linearize_eigval_eigvec(eigval_df, eigvec_df):
        res = linearize_df(eigvec_df, "f")
        #
        eigval_df = eigval_df.copy()
        eigval_df.index = ["eigval%s" % i for i in range(eigval_df.shape[0])]
        res = res.append(eigval_df)
        return res

    def _execute(self, df, ts):
        dbg.dassert_monotonic_index(df)
        # Compute correlation.
        df = exp.handle_nans(df, self.nan_mode_in_data)
        corr_df = df.corr()
        corr_df = exp.handle_nans(corr_df, self.nan_mode_in_corr)
        _LOG.debug("corr_df=%s", corr_df)
        # Use the last datetime as timestamp.
        dt = df.index.max()
        _LOG.debug("ts=%s", dt)
        # Compute eigenvalues and eigenvectors.
        # TODO(Paul): Consider replacing `eig` with `eigh` as per
        # https://stackoverflow.com/questions/45434989/numpy-difference-between-linalg-eig-and-linalg-eigh
        eigval, eigvec = np.linalg.eig(corr_df)
        #eigval, eigvec = np.linalg.eigh(corr_df)
        # Sort eigenvalues, if needed.
        if self.sort_eigvals:
            _, eigval, eigvec = self.sort_eigval(eigval, eigvec)
        _LOG.debug("eigval=\n%s\neigvec=\n%s", eigval, eigvec)
        # Package eigenvalues.
        eigval_df = pd.DataFrame([eigval], index=[dt])
        eigval_df = eigval_df.multiply(1 / eigval_df.sum(axis=1), axis="index")
        # Package eigenvectors.
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
                # Check if they are stable.
                num_fails = self.are_eigenvectors_stable(
                    prev_eigvec_df, eigvec_df)
                if num_fails:
                    _LOG.debug(
                        "Eigenvalues not stable: prev_ts=%s"
                        "\nprev_eigvec_df=\n%s"
                        "\neigvec_df=\n%s"
                        "\nnum_fails=%s", prev_ts, prev_eigvec_df, eigvec_df,
                        num_fails)
                    col_map, _ = self.stabilize_eigvec(prev_eigvec_df, eigvec_df)
                    shuffled_eigval_df, shuffled_eigvec_df = \
                        self.shuffle_eigval_eigvec(
                            eigval_df, eigvec_df, col_map)
                    # Check.
                    num_fails = self.are_eigenvectors_stable(
                        prev_eigvec_df, shuffled_eigvec_df)
                    dbg.dassert_eq(num_fails, 0)
                    eigval_df = shuffled_eigval_df
                    eigvec_df = shuffled_eigvec_df
        # Store.
        self._ts.append(ts)
        _LOG.debug("eigval_df=%s", eigval_df)
        self._eigval_df[ts] = eigval_df
        _LOG.debug("eigvec_df=\n%s", eigvec_df)
        self._eigvec_df[ts] = eigvec_df
        # Turn results into a pd.Series.
        self.linearize_eigval_eigvec(eigval_df, eigvec_df)
        return eigvec_df

    @staticmethod
    def sort_eigval(eigval, eigvec):
        are_eigval_sorted = (np.diff(eigval) <= 0).all()
        if not are_eigval_sorted:
            _LOG.debug("eigvals not sorted:\neigval=\n%s\neigvec=\n%s", eigval,
                       eigvec)
            # Sort eigvals in descending order.
            idx = eigval.argsort()[::-1]
            eigval = eigval[idx]
            eigvec = eigvec[:, idx]
            # Make sure it's sorted in descending order.
            dbg.dassert_eq_all(eigval, np.sort(eigval)[::-1])
        return are_eigval_sorted, eigval, eigvec

    # TODO(gp): -> eig_distance
    @staticmethod
    def eigvec_distance(v1, v2):
        # TODO(gp): Maybe the max of the diff of the component is a better
        # metric.
        diff = np.linalg.norm(v1 - v2)
        #_LOG.debug("v1=%s\nv2=%s\ndiff=%s", v1, v2, diff)
        return diff

    @staticmethod
    def check_stabilized_eigvec(col_map, n):
        # Check that col_map contains a permutation of the index.
        col_map_src = col_map.keys()
        col_map_dst = [x[1] for x in col_map.values()]
        exp_idxs = list(range(n))
        dbg.dassert_eq_all(sorted(col_map_src), exp_idxs)
        dbg.dassert_eq_all(sorted(col_map_dst), exp_idxs)
        return True

    @staticmethod
    def stabilize_eigvec(prev_eigvec_df, eigvec_df):
        """
        Try to find a permutation and sign changes of the columns in
        `prev_eigvec_df` to ensure continuity with `eigvec_df`.

        :return: map column index of original eigvec to (sign, column index
            of transformed eigvec)
        """
        def dist(v1, v2):
            # return res.PcaFactorComputer.eigvec_distance(v1, v2)
            return 1 - cosine(v1, v2)

        dbg.dassert_monotonic_index(prev_eigvec_df)
        dbg.dassert_monotonic_index(eigvec_df)
        # Build a matrix with the distances between corresponding vectors.
        num_cols = prev_eigvec_df.shape[1]
        distances = np.zeros((num_cols, num_cols)) * np.nan
        for i in range(num_cols):
            for j in range(num_cols):
                distances[i, j] = dist(prev_eigvec_df.iloc[:, i],
                                       eigvec_df.iloc[:, j])
        _LOG.debug("distances=\n%s", distances)
        # Find the row with the max abs value for each column.
        max_abs_cos = np.argmax(np.abs(distances), axis=1)
        _LOG.debug("max_abs_cos=%s", max_abs_cos)
        signs = np.sign(distances[range(0, num_cols), max_abs_cos])
        signs = list(map(int, signs))
        _LOG.debug("signs=%s", signs)
        # Package the results into col_map.
        col_map = {k: (signs[k], max_abs_cos[k]) for k in range(0, num_cols)}
        _LOG.debug("col_map=%s", col_map)
        #
        PcaFactorComputer.check_stabilized_eigvec(col_map, num_cols)
        return col_map, distances

    @staticmethod
    def stabilize_eigvec2(prev_eigvec_df, eigvec_df):
        """
        Different implementation of `stabilize_eigvec()`.
        """
        def eigvec_coeff(v1, v2, thr=1e-3):
            for sign in (-1, 1):
                diff = PcaFactorComputer.eigvec_distance(v1, sign * v2)
                _LOG.debug("v1=\n%s\nv2=\n%s\n-> diff=%s", v1, sign * v2, diff)
                if diff < thr:
                    return sign
            return None

        dbg.dassert_monotonic_index(prev_eigvec_df)
        dbg.dassert_monotonic_index(eigvec_df)
        # TODO(gp): This code can be sped up by:
        # 1) keeping a running list of the v2 columns already mapped so that
        #    we don't have to check over and over.
        # 2) once we find a match between columns, move to the next one v1
        # For now we just care about functionality.
        num_cols = eigvec_df.shape[1]
        col_map = {}
        for i in range(num_cols):
            for j in range(num_cols):
                coeff = eigvec_coeff(prev_eigvec_df.iloc[:, i],
                                  eigvec_df.iloc[:, j])
                _LOG.debug("i=%s, j=%s, coeff=%s", i, j, coeff)
                if coeff:
                    _LOG.debug("i=%s -> j=%s", i, j)
                    dbg.dassert_not_in(
                        i, col_map, msg="i=%s col_map=%s" % (i, col_map))
                    col_map[i] = (coeff, j)
        # Sanity check.
        PcaFactorComputer.check_stabilized_eigvec(col_map, num_cols)
        # Add dummy var to keep the same interface of stabilize_eigvec.
        dummy = None
        return col_map, dummy

    @staticmethod
    def shuffle_eigval_eigvec(eigval_df, eigvec_df, col_map):
        """
        Transform the eigenvalues / eigenvectors according to a col_map
        returned by `stabilize_eigvec`.

        :return: updated eigvalues and eigenvectors
        """
        dbg.dassert_monotonic_index(eigval_df)
        dbg.dassert_monotonic_index(eigvec_df)
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
        dbg.dassert_monotonic_index(prev_eigvec_df)
        dbg.dassert_monotonic_index(eigvec_df)
        dbg.dassert_eq(prev_eigvec_df.shape, eigvec_df.shape,
                       "prev_eigvec_df=\n%s\neigvec_df=\n%s",
                       prev_eigvec_df, eigvec_df)
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

    @staticmethod
    def are_eigenvalues_stable(prev_eigval_df, eigval_df, thr=1e-3):
        _LOG.debug("prev_eigval_df=\n%s\neigval_df=\n%s\n", prev_eigval_df,
                   eigval_df)
        dbg.dassert_monotonic_index(prev_eigval_df)
        dbg.dassert_monotonic_index(eigval_df)
        dbg.dassert_eq(prev_eigval_df.shape, eigval_df.shape)
        are_stable = True
        diff = PcaFactorComputer.eigvec_distance(prev_eigval_df, eigval_df)
        _LOG.debug("diff=%s", diff)
        if diff > thr:
            _LOG.debug("diff=%s > thr=%s", diff, thr)
            are_stable = False
        return are_stable

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
