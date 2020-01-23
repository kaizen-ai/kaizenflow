import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

import core.data_adapters as adpt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class _TestAdapter:
    def __init__(self, n_rows: int = 100, n_cols: int = 5):
        self._frequency = "T"
        self._n_rows = n_rows
        self._n_cols = n_cols
        self._df = self._get_test_df()
        self._x_vars = self._df.columns[:-2].tolist()
        self._y_vars = self._df.columns[-2:].tolist()

    def _get_test_df(self) -> pd.DataFrame:
        np.random.seed(42)
        idx = pd.Series(
            pd.date_range("2010-01-01", "2010-01-03", freq=self._frequency)
        ).sample(self._n_rows)
        df = pd.DataFrame(np.random.randn(self._n_rows, self._n_cols), index=idx)
        df.index.name = "timestamp"
        df.columns = [f"col_{j}" for j in range(0, self._n_cols)]
        df.sort_index(inplace=True)
        df = df.asfreq(self._frequency)
        return df

    @staticmethod
    def _list_tuples_to_str(
        features_target_pairs: List[Tuple[pd.DataFrame, pd.DataFrame]]
    ) -> str:
        pairs = []
        for i, (feature, target) in enumerate(features_target_pairs):
            pairs.append(
                f"{i}\nfeatures:\n{feature.to_string()}\ntarget:\n{target.to_string()}"
            )
        return "\n".join(pairs)


class TestTransformToGluon(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))

    def test_transform_local_ts(self) -> None:
        ta = _TestAdapter()
        local_ts = pd.concat([ta._df, ta._df], keys=[0, 1])
        gluon_ts = adpt.transform_to_gluon(
            local_ts, ta._x_vars, ta._y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))

    def test_transform_series_target(self) -> None:
        ta = _TestAdapter()
        y_vars = ta._y_vars[-1:]
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))


class TestTransformFromGluon(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        df = adpt.transform_from_gluon(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        self.check_string(df.to_string())

    def test_correctness(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        inverted_df = adpt.transform_from_gluon(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        inverted_df = inverted_df.astype(np.float64)
        pd.testing.assert_frame_equal(ta._df, inverted_df)


class TestTransformToSklean(hut.TestCase):
    @staticmethod
    def _sklearn_input_to_str(sklearn_input) -> str:
        params = ["idx", "x_vars", "x_vals", "y_vars", "y_vals:\n{}"]
        format_str = ":\n{}\n".join(params)
        return format_str.format(*sklearn_input)

    def test_transform1(self) -> None:
        ta = _TestAdapter()
        df = ta._df.dropna()
        sklearn_input = adpt.transform_to_sklearn(df, ta._x_vars, ta._y_vars)
        self.check_string(self._sklearn_input_to_str(sklearn_input))


class TestTransformFromSklean(hut.TestCase):
    @staticmethod
    def _get_sklearn_data() -> Tuple[
        pd.Index, List[str], pd.DataFrame, List[str], pd.DataFrame
    ]:
        np.random.seed(42)
        ta = _TestAdapter()
        df = ta._df.dropna()
        idx = df.index
        x_vars = ta._x_vars
        x_vals = df[x_vars]
        y_vars = ta._y_vars
        y_vals = df[y_vars]
        y_hat = pd.DataFrame(np.random.randn(len(idx)))
        return idx, x_vars, x_vals, y_vars, y_vals, y_hat

    def test_transform1(self) -> None:
        sklearn_data = TestTransformFromSklean._get_sklearn_data()
        self.check_string("x:\n{}\ny:{}\ny_h:{}".format(*sklearn_data))
