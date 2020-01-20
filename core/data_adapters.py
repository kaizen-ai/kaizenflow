import abc
import logging
from typing import Any, Iterable, List, Tuple

import gluonts
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class _Adapter(abc.ABC):
    @abc.abstractmethod
    def transform(self) -> Any:
        pass

    @abc.abstractmethod
    def inverse_transform(self, obj: Any) -> Any:
        pass


class GluonAdapter(_Adapter):
    def __init__(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: Iterable[str],
        freq: str,
    ):
        self._df = df
        self._target_col = target_col
        self._feature_cols = feature_cols
        self._freq = freq

    def transform(self) -> gluonts.dataset.common.ListDataset:
        dbg.dassert_isinstance(self._df.index, pd.DatetimeIndex)
        df = self._df.sort_index()
        df = df.asfreq(self._freq)
        return gluonts.dataset.common.ListDataset(
            [
                {
                    gluonts.dataset.field_names.FieldName.TARGET: df[
                        self._target_col
                    ],
                    gluonts.dataset.field_names.FieldName.START: df.index[0],
                    gluonts.dataset.field_names.FieldName.FEAT_DYNAMIC_REAL: df[
                        self._feature_cols
                    ],
                }
            ],
            freq=self._freq,
        )

    def inverse_transform(
        self, gluon_ts: gluonts.dataset.common.ListDataset
    ) -> List[Tuple[pd.DataFrame, pd.Series]]:
        dfs = []
        for ts in iter(gluon_ts):
            target = gluonts.dataset.util.to_pandas(ts)
            target.name = self._target_col
            features = pd.DataFrame(ts["feat_dynamic_real"], index=target.index)
            features.columns = self._feature_cols
            features.index.name = self._df.index.name
            dfs.append((features, target))
        return dfs
