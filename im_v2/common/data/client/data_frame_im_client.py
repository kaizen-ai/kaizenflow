"""
Import as:

import im_v2.common.data.client.data_frame_im_client as imvcdcdfimc
"""

from typing import Any, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import im_v2.common.data.client as icdc
import im_v2.common.data.client.base_im_clients as imvcdcbimcl
import im_v2.common.data.client.full_symbol as imvcdcfusy


class DataFrameImClient(imvcdcbimcl.ImClientReadingMultipleSymbols):

    def __init__(
        self,
        df: pd.DataFrame,
        resample_1min: bool,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> None:
        """
        Initialise the class with a DataFrame.

        Example of input dataframe:
        ```
                                        full_symbol ... close  volume  feature1
        2021-07-26 13:42:00+00:00  binance:BTC_USDT     101.0     100       1.0
        2021-07-26 13:43:00+00:00  binance:BTC_USDT     101.0     100       1.0
        2021-07-26 13:44:00+00:00  binance:BTC_USDT     101.0     100       1.0
        ```
        """
        vendor = "data_frame"
        super().__init__(
            vendor, resample_1min, full_symbol_col_name=full_symbol_col_name
        )
        # Validate and set input dataframe.
        self._validate_df(df)
        self._df = df

    def _validate_df(self, df: pd.DataFrame) -> None:
        """
        Validate that input dataframe has correct format.
        """
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert(not df.empty)
        hpandas.dassert_index_is_datetime(df)
        columns = [
            self._full_symbol_col_name,
            "open",
            "high",
            "low",
            "close",
            "volume",
            "feature1",
        ]
        hdbg.dassert_eq(df.columns.tolist(), columns)
        hdbg.dassert(df[self._full_symbol_col_name].notna().all())

    @staticmethod
    def get_universe() -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        return []

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See the parent class for description.
        """
        # Filter by full symbols.
        full_symbols_mask = self._df[full_symbol_col_name].isin(full_symbols)
        data = self._df[full_symbols_mask]
        # Filter data by specified timestamps.
        if start_ts:
            data = data.loc[data.index >= start_ts]
        if end_ts:
            data = data.loc[data.index <= end_ts]
        return data