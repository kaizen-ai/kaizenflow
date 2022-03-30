from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
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

        Input data is indexed with numbers and looks like:
        ```
             timestamp      open        close    volume
        0    1631145600000  3499.01 ... 3496.36  346.4812
        1    1631145660000  3496.36     3501.59  401.9576
        2    1631145720000  3501.59     3513.09  579.5656
        ```
        """
        # Check df.
        # TODO(Grisha): @Dan add more assertions, e.g., check that it is not empty,
        # maybe even create a method `_validate_df(df)`.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        self._df = df
        # Init the parent class.
        vendor = "data_frame"
        self._resample_1min = resample_1min
        self._full_symbol_col_name = full_symbol_col_name
        super.__init__(vendor, self._resample_1min, self._full_symbol_col_name)


    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
    ) -> pd.DataFrame:
        """
        See the parent class for description.
        """
        # Filter by full symbols.
        full_symbols_mask = self._df[full_symbol_col_name].isin(full_symbols)
        data = self._df[full_symbols_mask]
        #
        if start_ts:
            start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
            # Filter by start timstamp.
            data = data[data["timestamp"] >= start_ts]
        if end_ts:
            end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
            # Filter by end timstamp.
            data = data[data["timestamp"] <= end_ts]