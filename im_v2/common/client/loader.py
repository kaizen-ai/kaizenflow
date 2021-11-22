import abc
import logging
from typing import Any, Optional

import pandas as pd

import helpers.dbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)

# Exchange::Symbol, e.g. `binance::BTC_USDT`.
FullSymbol = str


# TODO(Grisha): add methods `get_start(end)_ts_available()`, `get_universe()` #543.
class AbstractImClient(abc.ABC):
    @abc.abstractmethod
    def read_data(
        self,
        full_symbol: FullSymbol,
        *,
        normalize: bool = True,
        drop_duplicates: bool = True,
        resample_to_1_min: bool = True,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        
        data = self._read_data(
            full_symbol, start_ts=start_ts, end_ts=end_ts, **kwargs
        )
        if normalize:
            data = self._normalize_data(data)
        if drop_duplicates:
            data = hpandas.drop_duplicates(data)
        if resample_to_1_min:
            # TODO(Grisha): think how to fill missing column values that appear due to resampling.
            data = hpandas.resample_df(data, "T")
        # Verify that data is valid.
        self._dassert_is_valid(data)

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbol: FullSymbol,
        *,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        print(0)

    @abc.abstractmethod
    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        print(0)

    @staticmethod
    def _dassert_is_valid(df: pd.DataFrame) -> None:

        hpandas.dassert_index_is_datetime(df)
        hpandas.dassert_monotonic_index(df)
        # Verify that timezone info is correct.
        expected_tz = "US/Eastern"
        hdbg.dassert_eq(
            df.index.tzinfo,
            expected_tz,
            msg=f"`DatetimeIndex` must have timezone={expected_tz} instead of {df.index.tzinfo}"
        )
        # Verify that there are no duplicates in data.
        n_duplicated_rows = df[df.duplicated()].shape[0]
        hdbg.dassert_eq(
            n_duplicated_rows,
            0,
            msg=f"There are {n_duplicated_rows} duplicated rows in data"
        )
