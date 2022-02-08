"""
Import as:

import market_data.market_data_im_client as mdmdimcl
"""

import logging
from typing import Any, List, Optional

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import im_v2.common.data.client as icdc
import market_data.abstract_market_data as mdabmada

_LOG = logging.getLogger(__name__)


# TODO(gp): @Grisha -> ImClientMarketData and rename all the classes and files.
class MarketDataImClient(mdabmada.AbstractMarketData):
    """
    Implement a `MarketData` that uses a `ImClient` as backend.
    """

    def __init__(
        self, *args: Any, im_client: icdc.ImClient, **kwargs: Any
    ) -> None:
        """
        Constructor.
        """
        super().__init__(*args, **kwargs)
        hdbg.dassert_isinstance(im_client, icdc.ImClient)
        self._im_client = im_client

    def get_last_price(
        self,
        col_name: str,
        asset_ids: List[int],
    ) -> pd.Series:
        """
        This method overrides parent method in `MarketData`.

        In contrast with specific `MarketData` backends,
        `MarketDataImClient` uses the end of interval for date
        filtering.
        """
        last_end_time = self.get_last_end_time()
        _LOG.info("last_end_time=%s", last_end_time)
        # Get the data.
        df = self.get_data_at_timestamp(
            last_end_time,
            self._end_time_col_name,
            asset_ids,
        )
        # Convert the df of data into a series.
        hdbg.dassert_in(col_name, df.columns)
        last_price = df[[col_name, self._asset_id_col]]
        last_price.set_index(self._asset_id_col, inplace=True)
        last_price_srs = hpandas.to_series(last_price)
        hdbg.dassert_isinstance(last_price_srs, pd.Series)
        last_price_srs.index.name = self._asset_id_col
        last_price_srs.name = col_name
        hpandas.dassert_series_type_in(last_price_srs, [np.float64, np.int64])
        # TODO(gp): Print if there are nans.
        return last_price_srs

    def should_be_online(self, wall_clock_time: pd.Timestamp) -> bool:
        """
        See the parent class.
        """
        # TODO(gp): It should delegate to the ImClient.
        return True

    def _get_data(
        self,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        """
        See the parent class.
        """
        # `ImClient` uses the convention [start_ts, end_ts).
        # TODO(gp): Check this invariant.
        if not left_close:
            if start_ts is not None:
                # Add one millisecond to not include the left boundary.
                start_ts += pd.Timedelta(1, "ms")
        if right_close:
            if end_ts is not None:
                # Add one millisecond to include the right boundary.
                end_ts -= pd.Timedelta(1, "ms")
        # TODO(gp): call dassert_is_valid_start_end_timestamp
        if not asset_ids:
            # If asset ids are not provided, get universe as full symbols.
            full_symbols = self._im_client.get_universe()
        else:
            # Convert asset ids to full symbols to read `im` data.
            full_symbols = self._im_client.get_full_symbols_from_numerical_ids(
                asset_ids
            )
        # Load the data using `im_client`.
        market_data = self._im_client.read_data(
            full_symbols,
            start_ts,
            end_ts,
        )
        # TODO(Grisha): we should pass `full_symbol_column_name` here CMTask #822.
        # Add `asset_id` column.
        market_data.insert(
            0,
            self._asset_id_col,
            self._im_client.get_numerical_ids_from_full_symbols(
                market_data["full_symbol"]
            ),
        )
        if self._columns:
            # Select only specified columns.
            hdbg.dassert_is_subset(self._columns, market_data.columns)
            market_data = market_data[self._columns]
        if limit:
            # Keep only top N records.
            hdbg.dassert_lte(1, limit)
            market_data = market_data.head(limit)
        # Prepare data for normalization by the parent class.
        market_data = self._convert_data_for_normalization(market_data)
        return market_data

    def _convert_data_for_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert data to format required by normalization in parent class.

        :param df: IM data to transform
        ```
                                  full_symbol     close     volume
                      index
        2021-07-26 13:42:00  binance:BTC_USDT  47063.51  29.403690
        2021-07-26 13:43:00  binance:BTC_USDT  46946.30  58.246946
        2021-07-26 13:44:00  binance:BTC_USDT  46895.39  81.264098
        ```
        :return: transformed data
        ```
                        end_ts       full_symbol     close     volume             start_ts
        idx
        0  2021-07-26 13:42:00  binance:BTC_USDT  47063.51  29.403690  2021-07-26 13:41:00
        1  2021-07-26 13:43:00  binance:BTC_USDT  46946.30  58.246946  2021-07-26 13:42:00
        2  2021-07-26 13:44:00  binance:BTC_USDT  46895.39  81.264098  2021-07-26 13:43:00
        ```
        """
        # Move the index to the end ts column.
        df.index.name = "index"
        df = df.reset_index()
        hdbg.dassert_not_in(self._end_time_col_name, df.columns)
        df = df.rename(columns={"index": self._end_time_col_name})
        # `IM` data is assumed to have 1 minute frequency.
        hdbg.dassert_not_in(self._start_time_col_name, df.columns)
        # hdbg.dassert_eq(df.index.freq, "1T")
        df[self._start_time_col_name] = df[
            self._end_time_col_name
        ] - pd.Timedelta(minutes=1)
        return df

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        # We need to find the last timestamp before the current time. We use
        # `7D` but could also use all the data since we don't call the DB.
        # TODO(gp): SELECT MAX(start_time) instead of getting all the data
        #  and then find the max and use `start_time`
        timedelta = pd.Timedelta("7D")
        df = self.get_data_for_last_period(timedelta)
        _LOG.debug(
            hpandas.df_to_str(df, print_shape_info=True, tag="after get_data")
        )
        if df.empty:
            ret = None
        else:
            ret = df.index.max()
        _LOG.debug("-> ret=%s", ret)
        return ret
