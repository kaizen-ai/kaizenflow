import abc
import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

import core.data_adapters as adpt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class _TestAdapter(abc.ABC):
    @property
    def _df(self, n_rows: int = 10, n_cols: int = 3) -> pd.DataFrame:
        self._freq = "T"
        np.random.seed(42)
        idx = pd.Series(
            pd.date_range("2010-01-01", "2011-01-01", freq=self._freq)
        ).sample(n_rows)
        df = pd.DataFrame(np.random.randn(n_rows, n_cols), index=idx)
        df.index.name = "timestamp"
        df.columns = [f"col_{j}" for j in range(0, n_cols)]
        self._feature_cols = df.columns[:-1]
        self._target_col = df.columns[-1]
        return df

    @abc.abstractmethod
    def test_transform(self) -> None:
        pass

    @abc.abstractmethod
    def test_inverse_transform(self) -> None:
        pass

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


class TestGluonAdapter(_TestAdapter, hut.TestCase):
    def test_transform(self) -> None:
        ga = adpt.GluonAdapter(
            self._df, self._target_col, self._feature_cols, self._freq
        )
        gluon_ts = ga.transform()
        self.check_string(str(list(gluon_ts)))

    def test_inverse_transform(self) -> None:
        ga = adpt.GluonAdapter(
            self._df, self._target_col, self._feature_cols, self._freq
        )
        gluon_ts = ga.transform()
        dfs = ga.inverse_transform(gluon_ts)
        self.check_string(_TestAdapter._list_tuples_to_str(dfs))

    def test_correctness(self) -> None:
        ga = adpt.GluonAdapter(
            self._df, self._target_col, self._feature_cols, self._freq
        )
        gluon_ts = ga.transform()
        dfs = ga.inverse_transform(gluon_ts)
        features, target = dfs[0]
        inversed_df = pd.concat([features, target], axis=1)
        inversed_df = inversed_df.astype(np.float64)
        reindexed_df = self._df.sort_index().asfreq(self._freq)
        pd.testing.assert_frame_equal(reindexed_df, inversed_df)
