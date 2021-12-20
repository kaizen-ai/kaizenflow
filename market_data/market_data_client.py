from typing import Any, List, Optional

import pandas as pd

import helpers.dbg as hdbg
import im_v2.common.data.client as ivcdclcl
import market_data.market_data_interface as mdmadain


class MarketDataInterFace(mdmadain.AbstractMarketDataInterface):
    def __init__(
        self,
        *args: Any,
        im_client: ivcdclcl.AbstractImClient,
    ) -> None:
        super().__init__(*args)
        self._im_client = im_client

    def should_be_online(self, wall_clock_time: pd.Timestamp) -> bool:
        return True

    def _get_data(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[str]],
        left_close: bool,
        right_close: bool,
        normalize_data: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        full_symbols = asset_ids
        market_data = self._im_client.read_data(
            full_symbols,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if self._columns:
            hdbg.dassert_is_subset(self._columns, market_data.columns)
            market_data = market_data[self._columns]
        if limit:
            hdbg.dassert_lte(1, limit)
            market_data = market_data.head(limit)
        if normalize_data:
            market_data = self.process_data(market_data)
        return market_data

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        return NotImplementedError
