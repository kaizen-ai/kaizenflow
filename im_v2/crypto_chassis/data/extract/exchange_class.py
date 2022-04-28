"""
This file contains a class for providing interface to download data from
Crypto-Chassis.

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""

import pandas as pd

class CryptoChassisExchange:
    """
    A class for accessing Crypto-Chassis data.

    This class implements an access layer that retrieves data from
    specified exchange(s) via Crypto-Chassis REST API.
    """

    def __init__(self) -> None:
        self._endpoint = ...  # This is the so-called "base_url" from CCh docs.

    def build_url(data_type: str, exchange: str, currency_pair: str):
        """
        TODO(danya): @toma
        Example on an url here:
        https://github.com/crypto-chassis/cryptochassis-data-api-docs#market-depth

        Up until the query (i.e. `?`)

        Use an f-string.
        """
        return url

    def download_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
        *,
        depth: Optional[int] = None
    ) -> pd.DataFrame:
        # TODO(Danya): @toma put data type assertions here before calling `fetch_market_depth`
        return market_depth

    def _fetch_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
        depth: Optional[int],
    ) -> pd.DataFrame:
        """
        Example here: https://github.com/crypto-chassis/cryptochassis-data-api-
        docs#market-depth.
        """
        # TODO(danya): @toma Build an url and then add query parameters if they are provided;
        #   transform timestamp to string via imv2tauti.timestamp_to_talos_iso_8601 (added a TODO to move up);
        #   read the link at requests.get(url).json()['urls'][0]['url'], compression is 'gzip';
        #   separate the columns as in the notebook in CMTask1705;
        #   return raw data.
        #  Note: no need to handle pagination,
        return market_depth
