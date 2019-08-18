import io
import logging

import numpy as np
import pandas as pd

import core.pandas_helpers as pde
import helpers.dbg as dbg
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


def dedent(txt):
    txt_out = []
    for l in txt.split("\n"):
        l = l.rstrip(" ").lstrip(" ")
        if l:
            txt_out.append(l)
    return "\n".join(txt_out)




class TestPcaFactorComputer1(ut.TestCase):

    @staticmethod
    def get_ex1():
        df_str = dedent("""
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

    def test_stabilize_eigenvec1(self):
        # Get data.
        prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df = \
            self.get_ex1()
        # Check if they are stable.
        num_fails = res.PcaFactorComputer.are_eigenvectors_stable(
            prev_eigvec_df, eigvec_df)
        self.assertEqual(num_fails, 3)
        self.assertFalse(
            res.PcaFactorComputer.are_eigenvalues_stable(
                prev_eigval_df, eigval_df))
        #
        col_map = res.PcaFactorComputer.stabilize_eigvec(
            prev_eigvec_df, eigvec_df)
        #
        shuffled_eigval_df, shuffled_eigvec_df = \
            res.PcaFactorComputer.shuffle_eigval_eigvec(
            eigval_df, eigvec_df, col_map)
        #
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
