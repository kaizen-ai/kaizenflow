import logging
import time
from typing import Any, Optional
import pandas as pd
import requests
from pycoingecko import CoinGeckoAPI
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import common.download as ssandown
_LOG = logging.getLogger(__name__)
###ok
class CGDownloader(ssandown.DataDownloader):
    """
    Class for downloading Coingecko Data for ETH and BTC
    """
    # _UNIVERSE = {
    #     "coingecko": [
    #         "bitcoin",
    #         "ethereum",
    #     ]
    # }
    
    def download(
        self, id: str, from_timestamp: str, to_timestamp: str, *args: Any, **kwargs: Any
    ) -> ssandown.RawData:

        # for symbol in self._UNIVERSE["coingecko"]:
        cg = CoinGeckoAPI()
        data = cg.get_coin_market_chart_range_by_id(
        id= id,
        vs_currency= 'usd',
        from_timestamp= from_timestamp,
        to_timestamp= to_timestamp)

        price = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
        mc = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
        vol = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'total_volume'])
        price.set_index('timestamp', inplace=True)
        mc.set_index('timestamp', inplace=True)
        vol.set_index('timestamp', inplace=True)
        df = pd.concat([price, mc, vol], axis=1)
        df.reset_index(inplace=True)
        print(df.columns)

        _LOG.info(f"Downloaded data: \n\t {df.head()}")
        return ssandown.RawData(df)

print('Done')