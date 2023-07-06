"""
Import as:

import im_v2.ccxt.data.extract.extractor as ivcdexex
"""

import copy
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import ccxt
import ccxt.pro as ccxtpro
import pandas as pd
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import im_v2.common.data.extract.extractor as imvcdexex

_LOG = logging.getLogger(__name__)


class CcxtExtractor(imvcdexex.Extractor):
    """
    A class for accessing CCXT exchange data.

    This class implements an access layer that:
    - logs into an exchange using the proper credentials
    - retrieves data in multiple chunks to avoid throttling
    """

    def __init__(self, exchange_id: str, contract_type: str) -> None:
        """
        Construct CCXT extractor.

        :param exchange_id: CCXT exchange id to log into (e.g., 'binance')
        :param contract_type: spot or futures contracts to extract
        """
        _LOG.info("CCXT version: %s", ccxt.__version__)
        super().__init__()
        hdbg.dassert_in(
            contract_type,
            ["futures", "spot"],
            msg="Supported contract types: spot, futures",
        )
        self.contract_type = contract_type
        self.exchange_id = exchange_id
        # Initialize two instances of class since exchanges initialized
        #  using ccxtpro inherit from base class which uses aync methods
        #  we keep the ccxt.Exchange in _sync_exchange for backwards
        #  compatibility with our codebase.
        self._async_exchange = self.log_into_exchange(async_=True)
        self._sync_exchange = self.log_into_exchange(async_=False)
        self.currency_pairs = self.get_exchange_currency_pairs()
        self.vendor = "CCXT"

    @staticmethod
    def convert_currency_pair(
        currency_pair: str, *, exchange_id: str = None
    ) -> str:
        """
        Convert currency pair used for getting data from exchange.
        """
        # TODO(Vlad): This is a temporary fix for OKX. We should
        #   implement a more general solution.
        #   Also seems like it works for websocket and don't work for
        #   REST API(bid_ask) - need to investigate.
        if exchange_id == "okx":
            # E.g., USD_BTC -> USD-BTC.
            return currency_pair.replace("_", "-")
        else:
            # E.g., USD_BTC -> USD/BTC.
            return currency_pair.replace("_", "/")

    def log_into_exchange(
        self, async_: bool
    ) -> ccxt.Exchange:
        """
        Log into an exchange via CCXT (or CCXT pro) and return the
        corresponding `Exchange` object.

        :param async_: if True, returns CCXT pro Exchange with async support,
         classic, sync ccxt Exchange otherwise.
        """
        exchange_params: Dict[str, Any] = {}
        # Enable rate limit.
        exchange_params["rateLimit"] = True
        if self.contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        module = ccxtpro if async_ else ccxt
        exchange_class = getattr(module, self.exchange_id)
        # Using API keys was deprecated in #2919.
        exchange = exchange_class(exchange_params)
        return exchange

    def get_exchange_currency_pairs(self) -> List[str]:
        """
        Get all the currency pairs available for the exchange.
        """
        return list(self._sync_exchange.load_markets().keys())

    def _is_latest_kline_present(self, data: list) -> bool:
        """
        Check if the last minute timestamp is present in the
            raw websocket data.

        :param data: list of OHLCV bars
        :return: True if the last minute timestamp is found in the data
            False otherwise
        """
        # Get the unique timestamps from the data.
        timestamps = set([row[0] for row in data])
        # Round the timestamp to the nearest minute.
        timestamp_to_check = (
            pd.Timestamp.utcnow() - pd.Timedelta(minutes=1)
        ).replace(second=0, microsecond=0)
        timestamp_to_check = hdateti.convert_timestamp_to_unix_epoch(
            timestamp_to_check, unit="ms"
        )
        # Check if the timestamp is present in the data.
        return timestamp_to_check in timestamps

    def _download_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        bar_per_iteration: Optional[int] = 1000,
        sleep_time_in_secs: float = 0.25,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download minute OHLCV bars.

        :param currency_pair: a currency pair, e.g. "BTC_USDT"
        :param start_timestamp: starting point for data
        :param end_timestamp: end point for data (included)
        :param bar_per_iteration: number of bars per iteration
        :param sleep_time_in_secs: time in seconds between iterations
        :return: OHLCV data from CCXT
        """
        # TODO(Vlad): Temporary fix for OKX REST. Need to refactor.
        if exchange_id == "okx":
            currency_pair = currency_pair.replace("-", "/")
        # Assign exchange_id to make it symmetrical to other vendors.
        _ = exchange_id
        hdbg.dassert(
            self._sync_exchange.has["fetchOHLCV"],
            "Exchange %s doesn't has fetch_ohlcv method",
            self._sync_exchange,
        )
        hdbg.dassert_in(
            currency_pair,
            self.currency_pairs,
            "Currency pair is not present in exchange",
        )
        # Get the latest bars if no timestamp is provided.
        if end_timestamp is None and start_timestamp is None:
            return self._fetch_ohlcv(
                currency_pair, bar_per_iteration=bar_per_iteration
            )
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(
            end_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_isinstance(
            start_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_lte(
            start_timestamp,
            end_timestamp,
        )
        # Convert datetime into ms.
        start_timestamp = start_timestamp.asm8.astype(int) // 1000000
        end_timestamp = end_timestamp.asm8.astype(int) // 1000000
        duration = self._sync_exchange.parse_timeframe("1m") * 1000
        all_bars = []
        # Iterate over the time period.
        # Note: the iteration goes from start date to end date in milliseconds,
        # with the step defined by `bar_per_iteration` parameter.
        # Because of this, the output can go slightly over the end date.
        for t in tqdm.tqdm(
            range(
                start_timestamp,
                end_timestamp + duration,
                duration * bar_per_iteration,
            )
        ):
            bars = self._fetch_ohlcv(
                currency_pair, since=t, bar_per_iteration=bar_per_iteration
            )
            all_bars.append(bars)
            time.sleep(sleep_time_in_secs)
        all_bars_df = pd.concat(all_bars)
        # Remove bars which are not part of the requested timerange to
        #  avoid confusion of receiving unsolicited data.
        all_bars_df = all_bars_df[
            (all_bars_df["timestamp"] >= start_timestamp)
            & (all_bars_df["timestamp"] <= end_timestamp)
        ]
        # It can happen that the received the data are not ordered by timestamp, which would
        #  make the is_monotonic check fail.
        all_bars_df = all_bars_df.sort_values("timestamp").reset_index(drop=True)
        hdbg.dassert(all_bars_df.timestamp.is_monotonic)
        # TODO(gp): Double check if dataframes are properly concatenated.
        return all_bars_df

    async def _subscribe_to_websocket_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        since: int,
        *,
        timeframe: str = "1m",
        limit: Optional[int] = 5,
        **kwargs: Any,
    ) -> None:
        """
        Wrapper to subscribe to OHLCV data via watchOHLCV websocket based
        approach.

        :param exchange_id: exchange to download from
         (not used, kept for compatibility with parent class).
        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :param since: from when is data fetched in UNIX epoch milliseconds
        :param timeframe: fetch data for certain timeframe
        :param limit: number of bars to return when getting OHLCV data from the
         websocket stream via ohlcvs dict, e.g. exchange.ohlcvs['currency_pair']
        """
        converted_pair = self.convert_currency_pair(
            currency_pair, exchange_id=exchange_id
        )
        await self._async_exchange.watchOHLCV(
            converted_pair, timeframe=timeframe, since=since, limit=limit
        )

    async def _subscribe_to_websocket_bid_ask(
        self,
        exchange_id: str,
        currency_pair: str,
        bid_ask_depth: int,
        **kwargs: Any,
    ) -> None:
        """
        Wrapper to subscribe to bid/ask (order book) data via CCXT pro
        watchOrderBook websocket based approach.

        :param exchange_id: exchange to download from
         (not used, kept for compatibility with parent class).
        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :param bid_ask_depth: how many levels of order book to download
        """
        currency_pair = self.convert_currency_pair(
            currency_pair, exchange_id=exchange_id
        )
        await self._async_exchange.watchOrderBook(
            currency_pair, limit=bid_ask_depth
        )

    async def _subscribe_to_websocket_trades(
        self, exchange_id: str, currency_pair: str, since: int, **kwargs: Any
    ) -> None:
        """
        Wrapper to subscribe to trades data via watchTrades websocket based
        approach.

        :exchange_id: exchange to download from
        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :param since:: from when is data fetched in UNIX epoch milliseconds
         websocket stream via trades dict, e.g. exchange.trades['currency_pair']
        """
        converted_pair = self.convert_currency_pair(
            currency_pair, exchange_id=exchange_id
        )
        await self._async_exchange.watchTrades(converted_pair, since=since)

    def _pad_bids_asks_to_equal_len(
        self, bids: List[List], asks: List[List]
    ) -> Tuple[List[List], List[List]]:
        """
        Pad list of bids and asks to the same length.
        """
        max_len = max(len(bids), len(asks))
        pad_bids_num = max_len - len(bids)
        pad_asks_num = max_len - len(asks)
        bids = bids + [[None, None]] * pad_bids_num
        asks = asks + [[None, None]] * pad_asks_num
        return bids, asks

    def _download_websocket_data(
        self, exchange_id: str, currency_pair: str, data_type: str
    ) -> Optional[Dict]:
        """
        Get the most recent websocket data for a specified currency pair and
        data type.

        :return Dict representing snapshot of the data for a specified symbol and data type.
        TODO(Juraj): show example
        """
        data = {}
        try:
            pair = self.convert_currency_pair(
                currency_pair, exchange_id=exchange_id
            )
            # TODO(Vlad): This is a temporary fix for OKX, which uses a different
            #  symbol format than the rest of the exchanges.
            #  Need to be refactored to a more general solution.
            if exchange_id == "okx":
                pair = pair.replace("-", "/")
            if data_type == "ohlcv":
                data = copy.deepcopy(self._async_exchange.ohlcvs[pair])
                for key in data:
                    # One of the returned key:value pairs is:
                    #  "timeframe": [o, h, l, c, v] where timeframe is e.g. '1m' and
                    #  o, h, l, c, v are the actual numerical values, 99,9% of time
                    #  '1m' is used but this is a cosmetic generalization to also support
                    #  '2m', '5m' etc.
                    if isinstance(
                        data[key], ccxtpro.base.cache.ArrayCacheByTimestamp
                    ):
                        ohlcv = data[key]
                # TODO(Vlad, Juraj): Need to modify during #3194
                if not self._is_latest_kline_present(ohlcv):
                    _LOG.warning(
                        f"Latest kline is not present in the downloaded data."
                        f" currency_pair={currency_pair}."
                    )
                data["ohlcv"] = ohlcv
                data["currency_pair"] = pair
            elif data_type == "bid_ask":
                if self._async_exchange.orderbooks.get(pair):
                    # CCXT uses their own 'dict-like' structure for storing the data
                    #  deepcopy is needed to retain the older data.
                    data = copy.deepcopy(
                        self._async_exchange.orderbooks[pair].limit()
                    )
                    # It can happen that the length of bids and asks does not match
                    #  it that case the shorter side gets padded wit Nones to equal length.
                    #  This minor preprocessing is performed this early to make transormations
                    #  simpler later down the pipeline.
                    if data.get("bids") != None and data.get("asks") != None:
                        (
                            data["bids"],
                            data["asks"],
                        ) = self._pad_bids_asks_to_equal_len(
                            data["bids"], data["asks"]
                        )
                        # TODO(Vlad): This is a temporary fix for OKX exchange, which
                        #  doesn't limit the number of bids and asks.
                        #  Need to be refactored to use depth as parameter.
                        if exchange_id == "okx":
                            data["bids"] = data["bids"][:10]
                            data["asks"] = data["asks"][:10]
                        # For some reason the OKX exchange does not return the symbol.
                        # This is a temporary fix for that.
                        if exchange_id == "okx" and data["symbol"] is None:
                            data["symbol"] = pair
                        if not isinstance(data.get("timestamp"), int):
                            _LOG.warning(
                                "Timestamp is not an integer. "
                                "currency_pair=%s.",
                                currency_pair,
                            )
                else:
                    data = None
            elif data_type == "trades":
                if self._async_exchange.trades.get(pair):
                    # In case of trades, the data is stored in a list of dicts.
                    # We convert it to a dict of lists for easier processing.
                    data = dict(
                        data=copy.deepcopy(self._async_exchange.trades[pair])
                    )
                    data["currency_pair"] = pair
                else:
                    data = None
            else:
                raise ValueError(
                    f"{data_type} not supported. Supported data types: ohlcv, bid_ask"
                )
            if data:
                data["end_download_timestamp"] = str(
                    hdateti.get_current_time("UTC")
                )
            return data
        except KeyError as e:
            _LOG.error(
                f"Websocket {data_type} data for {exchange_id} {currency_pair} is not available. "
                + "Have you subscribed to the websocket?"
            )
            raise e

    def _download_websocket_ohlcv(
        self, exchange_id: str, currency_pair: str
    ) -> Dict:
        """
        Get the most recent OHLCV data for a given currency pair.

        :return Dict representing snapshot of the data for a specified symbol and data type.
        TODO(Juraj): show example
        """
        return self._download_websocket_data(exchange_id, currency_pair, "ohlcv")

    def _download_websocket_bid_ask(
        self,
        exchange_id: str,
        currency_pair: str,
    ) -> Dict:
        """
        Get the most recent bid/ask (order book) data for a given currency pair
        for levels up to the specified limit.

        :return Dict representing snapshot of the order book
         for a specified currency pair up to limit-th level.
        TODO(Juraj): show example
        """
        return self._download_websocket_data(
            exchange_id, currency_pair, "bid_ask"
        )

    def _download_bid_ask(
        self, exchange_id: str, currency_pair: str, depth: int, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Download bid-ask data from CCXT.

        :param exchange_id: exchange to download from
         (not used, kept for compatibility with parent class).
        :param currency_pair: currency pair to download, i.e. BTC_USDT.
        :param depth: depth of the order book to download.
        """
        # TODO(Vlad): Temporary fix for OKX REST. Need to refactor.
        if exchange_id == "okx":
            currency_pair = currency_pair.replace("-", "/")
        # TODO(Juraj): can we get rid of this duplication of information?
        # exchange_id is set in constructor.
        # Assign exchange_id to make it symmetrical to other vendors.
        _ = exchange_id
        hdbg.dassert(
            self._sync_exchange.has["fetchOrderBook"],
            "Exchange %s doesn't has fetchOrderBook method",
            self._sync_exchange,
        )
        # Convert symbol to CCXT format, e.g. "BTC_USDT" -> "BTC/USDT".
        currency_pair = self.convert_currency_pair(
            currency_pair,
        )
        hdbg.dassert_in(
            currency_pair,
            self.currency_pairs,
            "Currency pair is not present in exchange",
        )
        # Download order book data.
        order_book = self._sync_exchange.fetch_order_book(currency_pair, depth)
        order_book["end_download_timestamp"] = str(
            hdateti.get_current_time("UTC")
        )
        order_book = pd.DataFrame.from_dict(order_book)
        # Separate price and size into columns.
        order_book[["bid_price", "bid_size"]] = pd.DataFrame(
            order_book.bids.to_list(), index=order_book.index
        )
        order_book[["ask_price", "ask_size"]] = pd.DataFrame(
            order_book.asks.to_list(), index=order_book.index
        )
        order_book["level"] = order_book.index + 1
        # Select bid/ask columns.
        bid_ask_columns = [
            "timestamp",
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
            "end_download_timestamp",
            "level",
        ]
        bid_ask = order_book[bid_ask_columns]
        return bid_ask

    def _download_websocket_trades(
        self, exchange_id: str, currency_pair: str
    ) -> Dict:
        """
        Get the most recent trades data for a given currency pair.

        :return Dict representing snapshot of the data for
          a specified symbol and data type.
          Example:
            {
                'data':[
                    {
                        'info': {
                            'e': 'trade',
                            'E': 1678440475956,
                            'T': 1678440475951,
                            's': 'ETHUSDT',
                            't': 2764874199,
                            'p': '1405.20',
                            'q': '0.242',
                            'X': 'MARKET',
                            'm': False
                        },
                        'timestamp': 1678440475951,
                        'datetime': '2023-03-10T09:27:55.951Z',
                        'symbol': 'ETH/USDT',
                        'id': '2764874199',
                        'order': None,
                        'type': None,
                        'side': 'buy',
                        'takerOrMaker': 'taker',
                        'price': 1405.2,
                        'amount': 0.242,
                        'cost': 340.0584,
                        'fee': None,
                        'fees': []
                    }
                ]
                'currency_pair': 'ETH/USDT'
                'end_download_timestamp': '2023-03-10 09:30:26.296282+00:00'
            }
        """
        return self._download_websocket_data(exchange_id, currency_pair, "trades")

    def _download_trades(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        sleep_time_in_secs: Optional[float] = 0.25,
        trade_per_iteration: Optional[int] = 1000,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download trades data from CCXT.

        :param exchange_id: exchange to download from
         (not used, kept for compatibility with parent class)
        :param currency_pair: currency pair to download, i.g. BTC_USDT
        :param start_timestamp: start timestamp of the data to download
        :param end_timestamp: end timestamp of the data to download
        :param sleep_time_in_secs: time to sleep between iterations
            It's not necessary to sleep, but it's a good idea to avoid
            overloading the exchange.
        :param trade_per_iteration: number of trades to download per iteration
            Add trade_per_iteration gain more control over the number of trades.
        :return: trades data
        """
        # TODO(Vlad): Temporary fix for OKX REST. Need to refactor.
        if exchange_id == "okx":
            currency_pair = currency_pair.replace("-", "/")
        elif exchange_id == "kraken":
            # Check if sleep time is not too low.
            # https://support.kraken.com/hc/en-us/articles/206548367-What-are-the-API-rate-limits-
            if sleep_time_in_secs < 1:
                _LOG.warning(
                    "Kraken has a limit of calling the public endpoints "
                    "at a frequency of 1 per second "
                    "Sleep time is set to 1 second."
                )
                sleep_time_in_secs = 1
        # Assign exchange_id to make it symmetrical to other vendors.
        _ = exchange_id
        hdbg.dassert(
            self._sync_exchange.has["fetchTrades"],
            "Exchange %s doesn't has fetchTrades method",
            self._sync_exchange,
        )
        # Convert symbol to CCXT format, e.g. "BTC_USDT" -> "BTC/USDT".
        currency_pair = self.convert_currency_pair(
            currency_pair,
        )
        hdbg.dassert_in(
            currency_pair,
            self.currency_pairs,
            "Currency pair is not present in exchange",
        )
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(
            end_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_isinstance(
            start_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_lte(
            start_timestamp,
            end_timestamp,
        )
        # Fetch trades.
        trades = self._fetch_trades(
            currency_pair,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            sleep_time_in_secs=sleep_time_in_secs,
            limit=trade_per_iteration,
        )
        return trades

    def _fetch_ohlcv(
        self,
        currency_pair: str,
        *,
        timeframe: str = "1m",
        since: Optional[int] = None,
        bar_per_iteration: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Wrapper for fetching one minute OHLCV bars.

        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :param timeframe: fetch data for certain timeframe
        :param since: from when is data fetched in milliseconds
        :param bar_per_iteration: number of bars per iteration

        :return: OHLCV data from CCXT that looks like:
            ```
                 timestamp      open      high       low     close    volume            end_download_timestamp
        0    1631145600000  46048.31  46050.00  46002.02  46005.10  55.12298  2022-02-22 18:00:06.091652+00:00
        1    1631145660000  46008.34  46037.70  45975.59  46037.70  70.45695  2022-02-22 18:00:06.091652+00:00
            ```
        """
        # Change currency pair to CCXT format.
        currency_pair = currency_pair.replace("_", "/")
        # Fetch the data through CCXT.
        bars = self._sync_exchange.fetch_ohlcv(
            currency_pair,
            timeframe=timeframe,
            since=since,
            limit=bar_per_iteration,
        )
        # Package the data.
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        bars = pd.DataFrame(bars, columns=columns)
        bars["end_download_timestamp"] = str(hdateti.get_current_time("UTC"))
        return bars

    def _fetch_trades(
        self,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        sleep_time_in_secs: Optional[float] = 0.25,
        limit: Optional[int] = 1000,
    ) -> pd.DataFrame:
        """
        Fetch trades with the ccxt method.

        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :param start_timestamp: start timestamp of the data to download.
        :param end_timestamp: end timestamp of the data to download.
        :param sleep_time_in_secs: time to sleep between iterations
        :param limit: number of trades per iteration
        :return: trades data from CCXT that looks like:
            ```
                timestamp      symbol      side    price     amount           end_download_timestamp
            0   1631145600000  BTC/USDT    buy     46048.31  0.001  2022-02-22 18:00:06.091652+00:00
            1   1631145600000  BTC/USDT    sell    46050.00  0.001  2022-02-22 18:00:06.091652+00:00
            ```
        """
        # Prepare parameters.
        columns = ["id", "timestamp", "symbol", "side", "price", "amount"]
        start_timestamp = hdateti.convert_timestamp_to_unix_epoch(start_timestamp)
        end_timestamp = hdateti.convert_timestamp_to_unix_epoch(end_timestamp)
        trades = []
        last_data_id: Optional[int] = None
        kraken_since: Optional[int] = None
        # Fetch the data through CCXT.
        while True:
            if last_data_id is None:
                # Start from beginning, get the data from the start timestamp.
                data = self._sync_exchange.fetch_trades(
                    currency_pair,
                    since=start_timestamp,
                    limit=limit,
                )
            else:
                # Pick up where we left off, get the data from the last id.
                if self.exchange_id == "kraken":
                    # Kraken doesn't support the limit parameter.
                    # And for iterations after the first one, we need to use
                    # the timestamp of the last trade as the since parameter.
                    # https://github.com/ccxt/ccxt/issues/5698
                    data = self._sync_exchange.fetch_trades(
                        currency_pair, since=kraken_since
                    )
                else:
                    params = {"fromId": last_data_id}
                    data = self._sync_exchange.fetch_trades(
                        currency_pair, limit=limit, params=params
                    )
            if self.exchange_id == "kraken" and len(data) > 0:
                kraken_since = int(data[-1]["timestamp"]) + 1
            df = pd.DataFrame(data, columns=columns)
            trades.append(df)
            # Stop if there is no data.
            if df.empty:
                break
            # Stop if dataset has reached the end of the time range.
            if (df.timestamp > end_timestamp).any():
                break
            # Update the fromId parameter as next iteration's starting point.
            last_data_id = int(df["id"].iloc[-1]) + 1
            # Take a nap in order to avoid hitting the rate limit.
            time.sleep(sleep_time_in_secs)
        trades_df = pd.concat(trades).reset_index(drop=True).drop(columns=["id"])
        trades_df["end_download_timestamp"] = str(hdateti.get_current_time("UTC"))
        # Cut the data if it exceeds the end timestamp.
        trades_df = trades_df[trades_df["timestamp"] < end_timestamp]
        return trades_df
