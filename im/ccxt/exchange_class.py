from typing import Dict, Optional

import ccxt

import helpers.dbg as dbg
import helpers.io_ as hio

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

    def load_api_credentials(self):
        """
        Load JSON file with available ccxt credentials
        :return: JSON file with API credentials
        """
        dbg.dassert_file_extension(self.api_keys_path, "json")
        all_credentials = hio.from_json(self.api_keys_path)
        return all_credentials

    def log_into_exchange(self) -> Dict[Dict[str, str]]:
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
