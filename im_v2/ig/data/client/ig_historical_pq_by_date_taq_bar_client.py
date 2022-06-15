"""
Import as:

import im_v2.ig.data.client.ig_historical_pq_by_date_taq_bar_client as imvidcihpbdtbc
"""

import logging
from typing import Any, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import im_v2.common.data.client as imvcdcli
import im_v2.common.universe as ivcu
import im_v2.ig.data.client.historical_bars as imvidchiba

_LOG = logging.getLogger(__name__)


class IgHistoricalPqByDateTaqBarClient(imvcdcli.HistoricalPqByDateClient):
    """
    Read historical TAQ bar data stored as Parquet by-date.

    The Parquet by-date data is stored at:
    ```
    > aws s3 ls --profile saml-spm-sasm \
            s3://iglp-data/.../taq/v1.0-prod/60
       PRE 20031002/
       PRE 20031003/
       ...
       PRE 20220111/
       PRE 20220112/
    ```

    This layer uses low-level functions from `historical_bars` to access the data.
    """

    def __init__(
        self,
        vendor: str,
        resample_1min: bool,
        root_dir: str,
        aws_profile: Optional[str],
        full_symbol_col_name: str,
    ):
        self._root_dir = root_dir
        self._aws_profile = aws_profile
        read_func = imvidchiba.get_bar_data_for_date_interval
        super().__init__(
            vendor,
            resample_1min,
            read_func,
            full_symbol_col_name=full_symbol_col_name,
        )

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        raise NotImplementedError

    # For IG `asset_id` and `full_symbol` have the same values but with
    # different types (int vs str).
    @staticmethod
    def get_asset_ids_from_full_symbols(
        full_symbols: List[ivcu.FullSymbol],
    ) -> List[int]:
        numeric_asset_id = list(map(int, full_symbols))
        return numeric_asset_id

    def get_full_symbols_from_asset_ids(
        self, asset_ids: List[int]
    ) -> List[ivcu.FullSymbol]:
        _ = self
        full_symbols = list(map(str, asset_ids))
        return full_symbols

    # Same as IgHistoricalPqByTileTaqBarClient.
    def get_universe(self) -> List[ivcu.FullSymbol]:
        # We don't need the mapping `asset_id -> full_symbol` since the IG
        # universe is already in the form of `asset_ids`, so we return an empty
        # universe here and override the functions doing the `asset_id <->
        # full_symbol` conversion.
        return []

    # /////////////////////////////////////////////////////////////////////////////

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        full_symbol_col_name: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Same as abstract method.
        """
        df = super()._read_data_for_multiple_symbols(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            full_symbol_col_name,
            root_data_dir=self._root_dir,
            aws_profile=self._aws_profile,
        )
        # Historical data doesn't have a knowledge time so we use the end of the
        # interval as a proxy for it, which for now it's the index.
        hdbg.dassert_not_in("timestamp_db", df.columns)
        # TODO(gp): we are considering keeping end_time as a column instead of an
        #  index.
        # df["timestamp_db"] = df["end_time"]
        df["timestamp_db"] = df.index
        return df
