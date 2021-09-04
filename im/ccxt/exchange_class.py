import helpers.io_ as hio
import helpers.dbg as dbg
import ccxt

class CCXTExchange:
    def __init__(self, exchange_id: str, api_keys_path: str):
        self.exchange_id = exchange_id
        self.api_keys_path = "/data/danya/src/cmamp1/im/ccxt/notebooks/API_keys.json"
        self.exchange = self.log_into_exchange()

    def log_into_exchange(self):
        """
        Log into exchange via ccxt.
        """
        # TODO (Danya): Update path to JSON API keys, factor out into separate loading function.
        credentials = hio.from_json(self.api_key_path)
        dbg.dassert_in(self.exchange_id, credentials, msg="'%s' exchange ID is incorrect.")
        credentials = credentials[exchange_id]
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class(credentials)
        dbg.dassert(exchange.checkRequiredCredentials(), msg="Required credentials not passed")
        return exchange