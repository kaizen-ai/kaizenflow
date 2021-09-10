import logging
import time
from typing import Any, Dict, List, Optional, Union

import ccxt

import helpers.dbg as dbg
import helpers.io_ as hio

dbg.init_logger(verbosity=logging.INFO)
_LOG = logging.getLogger(__name__)

API_KEYS_PATH = "/data/shared/data/API_keys.json"


class CCXTExchange:
    def __init__(
        self, exchange_id: str, api_keys_path: Optional[str] = None
    ) -> None:
        """
        Create a class for accessing ccxt exchange data.

        :param: exchange_id: ccxt exchange id
        :param: api_keys_path: path to json file with API credentials
        """
        self.exchange_id = exchange_id
        self.api_keys_path = api_keys_path or API_KEYS_PATH
        self._exchange = self.log_into_exchange()
        self.currency_pairs = self.get_exchange_currencies()

    def load_api_credentials(self) -> Dict[str, Dict[str, Union[str, bool]]]:
        """
        Load JSON file with available ccxt credentials
        :return: JSON file with API credentials
        """
        dbg.dassert_file_extension(self.api_keys_path, "json")
        all_credentials = hio.from_json(self.api_keys_path)
        return all_credentials

    def log_into_exchange(self) -> Any:
        """
        Log into exchange via ccxt.
        """
        # Load all exchange credentials.
        all_credentials = self.load_api_credentials()
        dbg.dassert_in(
            self.exchange_id,
            all_credentials,
            msg="'%s' exchange ID is incorrect.",
        )
        # Select credentials for provided exchange.
        credentials = all_credentials[self.exchange_id]
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, self.exchange_id)
        # Create a `ccxt` exchange class.
        exchange = exchange_class(credentials)
        dbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange

    def get_exchange_currencies(self):
        """
        Get all currency pairs available for exchange.
        """
        return self._exchange.load_markets().keys()

    def download_ohlcv_data(
        self,
        start_date: Union[int, str],
        end_date: Union[int, str],
        curr_symbol: str,
        step: Optional[int] = None,
        sleep_time: int = 1,
    ) -> List[List[Union[int, float]]]:
        """
        Download minute OHLCV candles.

        start_date and end_date should be passed as a datetime
        string in iso8601 format, e.g. '2019-02-19T00:00:00Z',
        or as a UNIX epoch time in ms, e.g. 1550534400000.

        :param start_date: starting point for data
        :param end_date: end point for data
        :param curr_symbol: a currency pair, e.g. "BTC/USDT"
        :param step: a number of candles per iteration
        :param sleep_time: time in seconds between iterations
        :return: OHLCV data from ccxt
        """
        # Verify that the exchange has fetch_ohlcv method.
        dbg.dassert(self._exchange.has["fetchOHLCV"])
        # Verify that the provided currency pair is present in exchange.
        dbg.dassert_in(curr_symbol, self.get_exchange_symbols())
        # Verify that date parameters are of correct format.
        dbg.dassert_isinstance(
            start_date, tuple([int, str]), msg="Type of start_date param is incorrect."
        )
        dbg.dassert_isinstance(
            end_date, tuple([int, str]), msg="Type of end_date param is incorrect."
        )
        # Make the minimal limit of 500 a default step.
        step = step or 500
        if isinstance(start_date, str):
            start_date = self._exchange.parse8601(start_date)
        if isinstance(end_date, str):
            end_date = self._exchange.parse8601(end_date)
        # Get 1m timeframe as millisecond.
        duration = self._exchange.parse_timeframe("1m") * 1000
        all_candles = []
        # Iterate over the time period.
        #  Note: the iteration goes from start date to end date in
        # milliseconds, with the step defined by `step` parameter.
        # Because of that, output can go slightly over the end_date,
        # since
        for t in range(start_date, end_date + duration, duration * step):
            # Fetch OHLCV candles for 1m since current datetime.
            candles = self._exchange.fetch_ohlcv(
                curr_symbol, timeframe="1m", since=t, limit=step
            )
            _LOG.info("Fetched %s candles" % len(candles))
            if candles:
                _LOG.info(
                    "From %s to %s" %
                    tuple([self._exchange.iso8601(candles[0][0]), self._exchange.iso8601(candles[-1][0])]))
            all_candles += candles
            _LOG.info("Fetched %s candles so far" % len(all_candles))
            time.sleep(sleep_time)
        return all_candles
