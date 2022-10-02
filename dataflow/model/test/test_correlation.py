import logging
from typing import Tuple

import pandas as pd
import pytest

import core.finance_data_example as cfidaexa
import dataflow.model.correlation as dtfmodcorr
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


def get_data(
    indices_equal: bool, columns_equal: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    start_timestamp = pd.Timestamp("2020-01-01-09:30")
    df1_end_timestamp = pd.Timestamp("2022-09-28-10:30")
    if indices_equal:
        df2_end_timestamp = df1_end_timestamp
    else:
        df2_end_timestamp = pd.Timestamp("2022-09-28-10:35")
    #
    df1_cols = [100, 200]
    if columns_equal:
        df2_cols = df1_cols
    else:
        df2_cols = [100, 200, 300]
    # NOTE: The data is not invariant with respect to changes in the start/end
    #  times or the columns.
    df1 = cfidaexa.get_forecast_dataframe(
        start_timestamp,
        df1_end_timestamp,
        df1_cols,
        seed=1,
    )
    df2 = cfidaexa.get_forecast_dataframe(
        start_timestamp,
        df2_end_timestamp,
        df2_cols,
        seed=2,
    )
    return df1, df2


class Test_compute_correlations(hunitest.TestCase):
    def test_equal_axes(self) -> None:
        df1, df2 = get_data(True, True)
        corrs = dtfmodcorr.compute_correlations(df1, df2)
        precision = 4
        actual = hpandas.df_to_str(
            corrs.round(precision), num_rows=None, precision=precision
        )
        expected = r"""
     prediction  returns  volatility
100     -0.0011   0.0016      0.0097
200      0.0013  -0.0031      0.0018
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_unequal_indices_failure(self) -> None:
        with pytest.raises(AssertionError):
            df1, df2 = get_data(False, True)
            _ = dtfmodcorr.compute_correlations(
                df1,
                df2,
            )

    def test_unequal_indices(self) -> None:
        df1, df2 = get_data(False, True)
        corrs = dtfmodcorr.compute_correlations(
            df1,
            df2,
            allow_unequal_indices=True,
        )
        precision = 4
        actual = hpandas.df_to_str(
            corrs.round(precision), num_rows=None, precision=precision
        )
        expected = r"""
     prediction  returns  volatility
100     -0.0044   0.0016      0.0097
200     -0.0015   0.0002      0.0057
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_unequal_columns_failure(self) -> None:
        df1, df2 = get_data(True, False)
        with pytest.raises(AssertionError):
            _ = dtfmodcorr.compute_correlations(df1, df2)

    def test_unequal_columns(self) -> None:
        df1, df2 = get_data(True, False)
        corrs = dtfmodcorr.compute_correlations(
            df1,
            df2,
            allow_unequal_columns=True,
        )
        precision = 4
        actual = hpandas.df_to_str(
            corrs.round(precision), num_rows=None, precision=precision
        )
        expected = r"""
     prediction  returns  volatility
100     -0.0011   0.0016      0.0097
200      0.0013  -0.0031      0.0018
300         NaN      NaN         NaN
"""
        self.assert_equal(actual, expected, fuzzy_match=True)