import logging

import pandas as pd

import core.statistics.empirical_distribution_function as cstaecdf
import core.statistics.random_samples as cstatrs
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_compute_empirical_cdf1(hunitest.TestCase):
    def test_gaussian(self) -> None:
        data = cstatrs.get_iid_standard_gaussian_samples(
            10,
            seed=0,
        )
        empirical_cdf = cstaecdf.compute_empirical_cdf(data)
        actual = get_df_as_str(empirical_cdf)
        expected = r"""
             ecdf
-1.26542  0.09091
-0.70374  0.18182
-0.53567  0.27273
-0.13210  0.36364
 0.10490  0.45455
 0.12573  0.54545
 0.36160  0.63636
 0.64042  0.72727
 0.94708  0.81818
 1.30400  0.90909
"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_compute_empirical_cdf_with_bounds1(hunitest.TestCase):
    def test_gaussian1(self) -> None:
        empirical_cdf_with_bounds = self.get_gaussian_empirical_cdf_with_bounds(
            n_samples=10,
            seed=0,
            alpha=0.2,
        )
        actual = get_df_as_str(empirical_cdf_with_bounds)
        expected = r"""
             ecdf  lower_CI  upper_CI
-1.26542  0.09091   0.00000   0.43022
-0.70374  0.18182   0.00000   0.52113
-0.53567  0.27273   0.00000   0.61203
-0.13210  0.36364   0.02433   0.70294
 0.10490  0.45455   0.11524   0.79385
 0.12573  0.54545   0.20615   0.88476
 0.36160  0.63636   0.29706   0.97567
 0.64042  0.72727   0.38797   1.00000
 0.94708  0.81818   0.47887   1.00000
 1.30400  0.90909   0.56978   1.00000
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_gaussian2(self) -> None:
        empirical_cdf_with_bounds = self.get_gaussian_empirical_cdf_with_bounds(
            n_samples=20,
            seed=1,
            alpha=0.1,
        )
        actual = get_df_as_str(empirical_cdf_with_bounds)
        expected = r"""
             ecdf  lower_CI  upper_CI
-1.30316  0.04762   0.00000   0.32129
-0.78191  0.09524   0.00000   0.36890
-0.73645  0.14286   0.00000   0.41652
-0.53695  0.19048   0.00000   0.46414
-0.48212  0.23810   0.00000   0.51176
-0.29246  0.28571   0.01205   0.55938
-0.25719  0.33333   0.05967   0.60700
-0.16291  0.38095   0.10729   0.65462
 0.02842  0.42857   0.15491   0.70224
 0.03972  0.47619   0.20252   0.74986
 0.29413  0.52381   0.25014   0.79748
 0.33044  0.57143   0.29776   0.84509
 0.34558  0.61905   0.34538   0.89271
 0.36457  0.66667   0.39300   0.94033
 0.44637  0.71429   0.44062   0.98795
 0.54671  0.76190   0.48824   1.00000
 0.58112  0.80952   0.53586   1.00000
 0.59885  0.85714   0.58348   1.00000
 0.82162  0.90476   0.63110   1.00000
 0.90536  0.95238   0.67871   1.00000
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def get_gaussian_empirical_cdf_with_bounds(
        self,
        n_samples,
        seed,
        alpha,
    ) -> pd.DataFrame:
        data = cstatrs.get_iid_standard_gaussian_samples(
            n_samples,
            seed
        )
        empirical_cdf = cstaecdf.compute_empirical_cdf_with_bounds(data, alpha)
        return empirical_cdf


def get_df_as_str(df: pd.DataFrame) -> pd.DataFrame:
    precision = 5
    str_df = hpandas.df_to_str(
        df.round(precision),
        num_rows=None,
        precision=precision,
    )
    return str_df