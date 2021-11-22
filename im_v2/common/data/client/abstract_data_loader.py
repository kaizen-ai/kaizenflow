import abc
import logging
from typing import Any, Optional

import pandas as pd

import helpers.dbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)

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
        """
        Read and process data for a single `FullSymbol` in [start_ts, end_ts),
        e.g. currency pair from a single exchange.

        Data processing includes:
            - normalization
            - dropping duplicates
            - resampling to 1 minute
            - validity verification

        :param full_symbol: `exchange::symbol`, e.g. `binance::BTC_USDT`
        :param normalize: transform data, e.g. rename columns, convert data types
        :param drop_duplicates: whether to drop full duplicates or not
        :param resample_to_1_min: whether to resample to 1 min or not
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: data for a single `FullSymbol` in [start_ts, end_ts)
        """
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
        """
        Read data for a single `FullSymbol` in [start_ts, end_ts), e.g.
        currency pair from a single exchange.

        :param full_symbol: `exchange::symbol`, e.g. `binance::BTC_USDT`
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: data for a single `FullSymbol` in [start_ts, end_ts)
        """

    @staticmethod
    @abc.abstractmethod
    def _normalize_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data, e.g. rename columns, convert data types.

        :param df: raw data
        :return: normalized data
        """

    @staticmethod
    def _dassert_is_valid(df: pd.DataFrame) -> None:
        """
        Verify that data is valid.

        Sanity checks include:
            - index is `pd.DatetimeIndex`
            - index is monotonic increasing/decreasing
            - index has timezone "US/Eastern"
            - data has no duplicates
        """
        hpandas.dassert_index_is_datetime(df)
        hpandas.dassert_monotonic_index(df)
        # Verify that timezone info is correct.
        # TODO(Grisha): converge on the tz `US/Eastern` vs `UTC`.
        expected_tz = "US/Eastern"
        hdbg.dassert_eq(
            df.index.tzinfo,
            expected_tz,
            msg=f"`DatetimeIndex` must have timezone={expected_tz} instead of {df.index.tzinfo}",
        )
        # Verify that there are no duplicates in data.
        n_duplicated_rows = df[df.duplicated()].shape[0]
        hdbg.dassert_eq(
            n_duplicated_rows,
            0,
            msg=f"There are {n_duplicated_rows} duplicated rows in data",
        )
