#!/usr/bin/env python
'''
API list:
1. /v1/cryptocurrency/listings/latest - Latest listings
2. /v2/cryptocurrency/quotes/latest - this endpoint to request the latest quote for specific cryptocurrencies.
3. /v1/exchange/map - CoinMarketCap ID map 
4. /v1/exchange/assets - Exchange Assets
5. /v1/global-metrics/quotes/latest - Latest global metrics
'''

import requests

class CMC_request():

    def get_data(api_num: int):
        url = self._build_url(self,api_num)
        try:
            response = requests.request(
                method="GET",
                url=url,
                headers={
                    "Content-Type": "application/json", 
                    "X-CMC_PRO_API_KEY": self.api_key, 
                    "Accepts": "application/json"},
                data={},
            )
            if response.json()['status']['error_code'] == 0:
                print("Request successful!")
                return response.json()['data']  
            else:
                print("Request failed, status code:", response.json()['status']['error_code'])
        except Exception as e:
            print("Request exception:", e)


    @staticmethod
    def _build_url(
        self,
        api_num: int,
        start:int = 1,
        convert:str = "USD",
        limit:int = 5000,
        id:str = "1",
        exchange_id:str = "270",
    ) -> str:
        if api_num == 1:
            # 1. /v1/cryptocurrency/listings/latest - Latest listings
            return (
                f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?"
                f"start={start}&convert={convert}&limit={limit}"
            )
        elif api_num == 2:
            # 2. /v2/cryptocurrency/quotes/latest - this endpoint to request the latest quote for specific cryptocurrencies.
            return (
                f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?"
                f"id={id}&convert={convert}"
            )
        elif api_num == 3:
            # 3. /v1/exchange/map - CoinMarketCap ID map
            return (
                f"https://pro-api.coinmarketcap.com/v1/exchange/map?"
                f"start={start}&limit={limit}"
            )
        elif api_num == 4:
            # 4. /v1/exchange/assets - Exchange Assets
            return (
                f"https://pro-api.coinmarketcap.com/v1/exchange/assets?"
                f"id={exchange_id}"
            )
        elif api_num == 5:
            # 5. /v1/global-metrics/quotes/latest - Latest global metrics
            return (
                f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?"
                f"convert={convert}"
            )
        else:
            print("Invalid API number, please use number between 1-5")




