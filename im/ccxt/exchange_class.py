import helpers.io_ as hio
import helpers.dbg as dbg
import ccxt
from typing import Optional

API_KEYS_PATH = "/data/shared/data/API_keys.json"


class CCXTExchange:
    def __init__(self, exchange_id: str, api_keys_path: Optional[str]) -> None:
        """
        Create a class for accessing ccxt exchange data.
        :param: exchange_id: ccxt exchange id
        :param: api_keys_path: path to json
        """
        self.exchange_id = exchange_id
        self.api_keys_path = api_keys_path or API_KEYS_PATH
        self.exchange = self.log_into_exchange()

    def log_into_exchange(self):
        """
        Log into exchange via ccxt.
        """
        # TODO (Danya): Update path to JSON API keys, factor out into separate loading function.
        all_credentials = hio.from_json(self.api_keys_path)
        dbg.dassert_in(self.exchange_id, all_credentials, msg="'%s' exchange ID is incorrect.")
        credentials = all_credentials[self.exchange_id]
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, self.exchange_id)
        exchange = exchange_class(credentials)
        dbg.dassert(exchange.checkRequiredCredentials(), msg="Required credentials not passed")
        return exchange
