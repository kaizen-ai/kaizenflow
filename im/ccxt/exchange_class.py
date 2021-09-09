from typing import Any, Dict, Optional
import time
import logging

import ccxt

import helpers.dbg as dbg
import helpers.io_ as hio

dbg.init_logger(verbosity=logging.INFO)
_LOG = logging.getLogger(__name__)

API_KEYS_PATH = "/data/shared/data/API_keys.json"


class CCXTExchange:
    def __init__(self, exchange_id: str, api_keys_path: Optional[str] = None) -> None:
        """
        Create a class for accessing ccxt exchange data.

        :param: exchange_id: ccxt exchange id
        :param: api_keys_path: path to json file with API credentials
        """
        self.exchange_id = exchange_id
        self.api_keys_path = api_keys_path or API_KEYS_PATH
        self.exchange = self.log_into_exchange()

    def load_api_credentials(self) -> Dict[str, Dict[str, str]]:
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
        dbg.dassert_in(self.exchange_id, all_credentials, msg="'%s' exchange ID is incorrect.")
        # Select credentials for provided exchange.
        credentials = all_credentials[self.exchange_id]
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, self.exchange_id)
        # Create a `ccxt` exchange class.
        exchange = exchange_class(credentials)
        dbg.dassert(exchange.checkRequiredCredentials(), msg="Required credentials not passed")
        return exchange

    def download_ohlcv_data(self,
                            start_date: int,
                            end_date: int,
                            curr_symbol: str,
                            step: Optional[int] = None,
                            sleep_time: int = 1):
        """

        :param start_date:
        :param end_date:
        :param curr_symbol:
        :param step:
        :param sleep_time:
        :return:
        """
        dbg.dassert(self.exchange.has["fetchOHLCV"])
        dbg.dassert_in(curr_symbol, self.exchange.load_markets().keys())
        start_date = self.exchange.parse8601(start_date)
        end_date = self.exchange.parse8601(end_date)
        # Convert to ms.
        duration = self.exchange.parse_timeframe("1m") * 1000
        all_candles = []
        for t in range(start_date, end_date+duration, duration*step):
            candles = self.exchange.fetch_ohlcv(curr_symbol, "1m", t, step)
            _LOG.info('Fetched', len(candles), 'candles')
            if candles:
                _LOG.info('From', self.exchange.iso8601(candles[0][0]), 'to', self.exchange.iso8601(candles[-1][0]))
            all_candles += candles
            total_length = len(all_candles)
            _LOG.info('Fetched', total_length, 'candles in total')
            time.sleep(sleep_time)
        return all_candles
