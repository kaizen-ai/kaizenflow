from typing import Any, List, Optional

import pandas as pd

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
        wall_clock_time = self.get_wall_clock_time()
        full_symbols = asset_ids
        market_data = self._im_client.read_data(
            full_symbols,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        return market_data

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        return NotImplementedError
