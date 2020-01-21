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
        self._x_vars = self._df.columns[:-2]
        self._y_vars = self._df.columns[-2:]

    def _get_test_df(self) -> pd.DataFrame:
        np.random.seed(42)
        idx = pd.Series(
            pd.date_range("2010-01-01", "2010-01-03", freq=self._frequency)
        ).sample(self._n_rows)
        df = pd.DataFrame(np.random.randn(self._n_rows, self._n_cols), index=idx)
        df.index.name = "timestamp"
        df.columns = [f"col_{j}" for j in range(0, self._n_cols)]
        df.sort_index(inplace=True)
        return df

    @staticmethod
    def _list_tuples_to_str(
        features_target_pairs: List[Tuple[pd.DataFrame, pd.Series]]
    ) -> str:
        pairs = []
        for i, (feature, target) in enumerate(features_target_pairs):
            pairs.append(
                f"{i}\nfeatures:\n{feature.to_string()}\ntarget:\n{target.to_string()}"
            )
        return "\n".join(pairs)


class TestTransformPandasGluon(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_pandas_gluon(
            ta._df, ta._frequency, ta._x_vars, ta._y_vars
        )
        self.check_string(str(list(gluon_ts)))

    def test_transform_local_ts(self) -> None:
        ta = _TestAdapter()
        local_ts = pd.concat([ta._df, ta._df], keys=[0, 1])
        gluon_ts = adpt.transform_pandas_gluon(
            local_ts, ta._frequency, ta._x_vars, ta._y_vars
        )
        self.check_string(str(list(gluon_ts)))


class TestTransformGluonPandas(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_pandas_gluon(
            ta._df, ta._frequency, ta._x_vars, ta._y_vars
        )
        dfs = adpt.transform_gluon_pandas(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        self.check_string(ta._list_tuples_to_str(dfs))

    def test_correctness(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_pandas_gluon(
            ta._df, ta._frequency, ta._x_vars, ta._y_vars
        )
        dfs = adpt.transform_gluon_pandas(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        targets = [target for _, target in dfs]
        features = dfs[0][0]
        inversed_df = pd.concat([features] + targets, axis=1)
        inversed_df = inversed_df.astype(np.float64)
        reindexed_df = ta._df.asfreq(ta._frequency)
        pd.testing.assert_frame_equal(reindexed_df, inversed_df)
