import io
import logging

import numpy as np
import pandas as pd

import core.pandas_helpers as pde
import helpers.dbg as dbg
import helpers.printing as pri
import helpers.unit_test as ut
import utilities.core.residualizer as res

_LOG = logging.getLogger(__name__)

# #############################################################################


class TestDfRollingApply(ut.TestCase):

    def test1(self):
        """
        Test with function returning a pd.Series.
        """
        np.random.seed(10)
        func = np.mean
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, func)
        #
        df_exp = df.rolling(window).apply(func, raw=True)
        self.assert_equal(df_act.to_string(), df_exp.to_string())

    def test2(self):
        """
        Test with function returning a pd.DataFrame.
        """
        np.random.seed(10)
        func = lambda x: pd.DataFrame(np.mean(x))
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, func)
        #
        func = np.mean
        df_exp = df.rolling(window).apply(func, raw=True)
        # Convert to an equivalent format.
        df_exp = pd.DataFrame(df_exp.stack(dropna=False))
        self.assert_equal(df_act.to_string(), df_exp.to_string())

    def test3(self):
        """
        Test with function returning a pd.DataFrame with multiple lines.
        """
        np.random.seed(10)
        func = lambda x: pd.DataFrame([np.mean(x), np.sum(x)])
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, func)
        self.check_string(df_act.to_string())


# #############################################################################


class TestPcaFactorComputer1(ut.TestCase):

    @staticmethod
    def get_ex1():
        df_str = pri.dedent("""
        ,0,1,2
        0,0.68637724274453,0.34344509725064354,0.6410395820984168
        1,-0.7208890365507423,0.205021903910637,0.6620309780499695
        2,-0.09594413803541411,0.916521404055221,-0.3883081743735094""")
        df_str = io.StringIO(df_str)
        prev_eigvec_df = pd.read_csv(df_str, index_col=0)
        prev_eigvec_df.index = prev_eigvec_df.index.map(int)
        prev_eigvec_df.columns = prev_eigvec_df.columns.map(int)
        #
        prev_eigval_df = pd.DataFrame([[1.0, 0.5, 0.3]], columns=[0, 1, 2])
        # Shuffle eigenvalues / eigenvectors.
        eigvec_df = prev_eigvec_df.copy()
        shuffle = [1, 2, 0]
        eigvec_df = eigvec_df.reindex(columns=shuffle)
        eigvec_df.columns = list(range(eigvec_df.shape[1]))
        eigvec_df.iloc[:, 1] *= -1
        #
        eigval_df = prev_eigval_df.reindex(columns=shuffle)
        eigval_df.columns = list(range(eigval_df.shape[1]))
        for obj in (prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df):
            dbg.dassert_monotonic_index(obj)
        return prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df

    def _test_stabilize_eigenvec_helper(self, data_func, eval_func):
        # Get data.
        prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df = \
            data_func()
        # Check if they are stable.
        num_fails = res.PcaFactorComputer.are_eigenvectors_stable(
            prev_eigvec_df, eigvec_df)
        self.assertEqual(num_fails, 3)
        # Transform.
        col_map, _ = eval_func(prev_eigvec_df, eigvec_df)
        #
        shuffled_eigval_df, shuffled_eigvec_df = \
                res.PcaFactorComputer.shuffle_eigval_eigvec(
                        eigval_df, eigvec_df, col_map)
        # Check.
        txt = ("prev_eigval_df=\n%s\n" % prev_eigval_df +
               "prev_eigvec_df=\n%s\n" % prev_eigvec_df +
               "eigval_df=\n%s\n" % eigval_df + "eigvec_df=\n%s\n" % eigvec_df +
               "shuffled_eigval_df=\n%s\n" % shuffled_eigval_df +
               "shuffled_eigvec_df=\n%s\n" % shuffled_eigvec_df)
        self.check_string(txt)
        # Check stability.
        num_fails = res.PcaFactorComputer.are_eigenvectors_stable(
            prev_eigvec_df, shuffled_eigvec_df)
        self.assertEqual(num_fails, 0)
        self.assertTrue(
            res.PcaFactorComputer.are_eigenvalues_stable(
                prev_eigval_df, shuffled_eigval_df))

    def test_stabilize_eigenvec1(self):
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer.stabilize_eigvec
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    def test_stabilize_eigenvec2(self):
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer.stabilize_eigvec2
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    # ##########################################################################

    def test_linearize_eigval_eigvec(self):
        # Get data.
        eigval_df, _, eigvec_df, _ = self.get_ex1()
        # Evaluate.
        out = res.PcaFactorComputer.linearize_eigval_eigvec(
            eigval_df, eigvec_df)
        _LOG.debug("out=\n%s", out)
        # Check.
        txt = ("eigval_df=\n%s\n" % eigval_df + "eigvec_df=\n%s\n" % eigvec_df +
               "out=\n%s" % out)
        self.check_string(txt)

    # ##########################################################################

    def _test_sort_eigval_helper(self, eigval, eigvec, are_eigval_sorted_exp):
        are_eigval_sorted, eigval_tmp, eigvec_tmp = \
            res.PcaFactorComputer.sort_eigval(eigval, eigvec)
        self.assertEqual(are_eigval_sorted, are_eigval_sorted_exp)
        self.assertSequenceEqual(eigval_tmp.tolist(),
                                 sorted(eigval_tmp, reverse=True))
        vars_as_str = [
            "eigval", "eigvec", "are_eigval_sorted", "eigval_tmp", "eigvec_tmp"
        ]
        txt = pri.vars_to_debug_string(vars_as_str, locals())
        self.check_string(txt)

    def test_sort_eigval1(self):
        eigval = np.array([1.30610138, 0.99251131, 0.70138731])
        eigvec = np.array([[-0.55546523, 0.62034663, 0.55374041],
                           [0.70270302, -0.00586218, 0.71145914],
                           [-0.4445974, -0.78430587, 0.43266321]])
        are_eigval_sorted_exp = True
        self._test_sort_eigval_helper(eigval, eigvec, are_eigval_sorted_exp)

    def test_sort_eigval2(self):
        eigval = np.array([0.99251131, 0.70138731, 1.30610138])
        eigvec = np.array([[-0.55546523, 0.62034663, 0.55374041],
                           [0.70270302, -0.00586218, 0.71145914],
                           [-0.4445974, -0.78430587, 0.43266321]])
        are_eigval_sorted_exp = False
        self._test_sort_eigval_helper(eigval, eigvec, are_eigval_sorted_exp)

# #############################################################################

import numpy as np
from scipy.linalg import eigh, cholesky
from scipy.stats import norm

import helpers.explore as exp
import matplotlib.pyplot as plt

class TestPcaFactorComputer2(ut.TestCase):

    @staticmethod
    def _get_data(num_samples, report_stats):
        # The desired covariance matrix.
        # r = np.array([
        #         [  3.40, -2.75, -2.00],
        #         [ -2.75,  5.50,  1.50],
        #         [ -2.00,  1.50,  1.25]
        #     ])
        cov = np.array([
            [1.0, 0.5, 0],
            [0.5, 1, 0],
            [0, 0, 1]
        ])
        if report_stats:
            _LOG.info("cov=\n%s", cov)
            exp.plot_heatmap(cov, mode="heatmap", title="cov")
            plt.show()
        # Generate samples from three independent normally distributed random
        # variables with mean 0 and std dev 1.
        np.random.seed(10)
        x = norm.rvs(size=(3, num_samples))
        if report_stats:
            _LOG.info("x=\n%s", x[:2, :])
        # We need a matrix `c` for which `c*c^T = r`.
        # We can use # the Cholesky decomposition, or the we can construct `c`
        # from the eigenvectors and eigenvalues.
        # Compute the eigenvalues and eigenvectors.
        # evals, evecs = np.linalg.eig(r)
        evals, evecs = np.linalg.eigh(cov)
        if report_stats:
            _LOG.info("evals=\n%s", evals)
            _LOG.info("evecs=\n%s", evecs)
            exp.plot_heatmap(evecs, mode="heatmap", title="evecs")
            plt.show()
        # Construct c, so c*c^T = r.
        transform = np.dot(evecs, np.diag(np.sqrt(evals)))
        if report_stats:
            _LOG.info("transform=\n%s", transform)
        # print(c.T * c)
        # print(c * c.T)
        # Convert the data to correlated random variables.
        y = np.dot(transform, x)
        y_cov = np.corrcoef(y)
        if report_stats:
            _LOG.info("cov(y)=\n%s", y_cov)
            exp.plot_heatmap(y_cov, mode="heatmap", title="y_cov")
            plt.show()
        #
        y = pd.DataFrame(y).T
        _LOG.debug("y=\n%s", y.head(5))
        result = {
            "y": y,
            "cov": cov,
            "evals": evals,
            "evecs": evecs,
            "transform": transform,
        }
        return result

    def _helper(self, num_samples, report_stats, stabilize_eig, window):
        result = self._get_data(num_samples, report_stats)
        _LOG.debug("result=%s", result.keys())
        #
        nan_mode_in_data = "drop"
        nan_mode_in_corr = "fill_with_zero"
        sort_eigvals = True
        comp = res.PcaFactorComputer(nan_mode_in_data, nan_mode_in_corr,
                                     sort_eigvals, stabilize_eig)
        df_res = pde.df_rolling_apply(result["y"], window, comp,
                                      progress_bar=True)
        if report_stats:
            comp.plot_over_time(df_res, num_pcs_to_plot=-1)
        return comp, df_res

    def _check(self, comp, df_res):
        txt = []
        txt.append("comp.get_eigval_names()=\n%s" % comp.get_eigval_names())
        txt.append("df_res.mean()=\n%s" % df_res.mean())
        txt.append("df_res.std()=\n%s" % df_res.std())
        txt = "\n".join(txt)
        self.check_string(txt)

    def test1(self):
        num_samples = 100
        report_stats = False
        stabilize_eig = False
        window = 50
        comp, df_res = self._helper(num_samples, report_stats, stabilize_eig,
                                    window)
        self._check(comp, df_res)

    def test2(self):
        num_samples = 100
        report_stats = False
        stabilize_eig = True
        window = 50
        comp, df_res = self._helper(num_samples, report_stats, stabilize_eig,
                                    window)
        self._check(comp, df_res)
