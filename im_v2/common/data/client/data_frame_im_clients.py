"""
Import as:

import im_v2.common.data.client.data_frame_im_clients as imvcdcdfimc
"""

from typing import Any, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import im_v2.common.data.client.base_im_clients as imvcdcbimcl
import im_v2.common.data.client.full_symbol as imvcdcfusy


class DataFrameImClient(imvcdcbimcl.ImClientReadingMultipleSymbols):
    """
    `ImClient` that serves data from a passed dataframe indexed with
    timestamps.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        universe: List[imvcdcfusy.FullSymbol],
        resample_1min: bool,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> None:
        """
        Constructor.

        Example of an input dataframe:

        ```
                                        full_symbol ... close  volume  feature1
        timestamp
        2021-07-26 13:42:00+00:00  binance:BTC_USDT     101.0     100       1.0
        2021-07-26 13:43:00+00:00  binance:BTC_USDT     101.0     100       1.0
        2021-07-26 13:44:00+00:00  binance:BTC_USDT     101.0     100       1.0
        ```
        """
        # Validate that the input universe is a non-empty list.
        hdbg.dassert_container_type(universe, list, imvcdcfusy.FullSymbol)
        hdbg.dassert_lte(1, len(universe))
        # Set the input universe before calling the parent class ctor since
        # it is used by `get_universe()` which is necessary for the parent
        # class initialisation.
        self._universe = sorted(universe)
        # Initialise the parent class.
        vendor = "data_frame"
        super().__init__(
            vendor, resample_1min, full_symbol_col_name=full_symbol_col_name
        )
        # Validate and set input dataframe.
        hpandas.dassert_time_indexed_df(
            df, allow_empty=False, strictly_increasing=False
        )
        self._df = df

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_universe(self) -> List[imvcdcfusy.FullSymbol]:
        """
        See description in the parent class.
        """
        return self._universe

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
