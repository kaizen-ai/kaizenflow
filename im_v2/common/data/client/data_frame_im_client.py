"""
Import as:

import im_v2.common.data.client.data_frame_im_client as imvcdcdfimc
"""

from typing import Any, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
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

        Input data is indexed with numbers and looks like: ```
        timestamp      open        close    volume 0    1631145600000
        3499.01 ... 3496.36  346.4812 1    1631145660000  3496.36
        3501.59  401.9576 2    1631145720000  3501.59     3513.09
        579.5656 ```
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
            "open",
            "high",
            "low",
            "close",
            "volume",
            self._full_symbol_col_name,
            "feature1",
        ]
        hdbg.dassert_is(df.columns, columns)
        hdbg.dassert(df[self._full_symbol_col_name].notna().all())

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