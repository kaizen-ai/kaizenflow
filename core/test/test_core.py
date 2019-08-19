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
        prev_eigval_df = pd.Series([1.0, 0.5, 0.3], index=[0, 1, 2])
        # Shuffle eigenvalues / eigenvectors.
        eigvec_df = prev_eigvec_df.copy()
        shuffle = [1, 2, 0]
        eigvec_df = eigvec_df.reindex(columns=shuffle)
        eigvec_df.columns = list(range(eigvec_df.shape[1]))
        eigvec_df.iloc[:, 1] *= -1
        #
        eigval_df = prev_eigval_df.reindex(index=shuffle)
        eigval_df.index = list(range(eigval_df.shape[0]))
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
