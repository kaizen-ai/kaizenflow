"""
API client helper for binance.
Ref: https://github.com/binance/binance-public-data/blob/master/Futures_Order_Book_Download/Futures-order-book-Level2-data-download.py

Import as:

import im_v2.binance.data.extract.api_client as imvbdeapcl
"""

import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import requests

import helpers.hsecrets as hsecret

_LOG = logging.getLogger(__name__)


class BinanceAPIClient:
    """
    API client for requesting historical bid-ask data from Binance.

    This client handles the generation of authenticated requests to the
    Binance API. It signs the request parameters using HMAC SHA256 and
    includes the necessary headers for authentication.
    """

    def __init__(self, secret_name: str):
        """
        Initializes the APIClient with API and secret keys.

        :param secret_name: secret_id to get api key and secret key
        """
        try:
            keys = hsecret.get_secret(secret_name)
            # Specify the api_key and secret_key with your API Key and secret_key
            self.api_key = keys["apiKey"]
            self.secret_key = keys["secret"]
        except:
            _LOG.warning("Invalid Secret Key : %s", secret_name)
            self.api_key = ""
            self.secret_key = ""

    def _sign(self, params={}):
        """
        Sign the request parameters using HMAC SHA256 algorithm.

        This function creates a timestamp, adds it to the parameters,
        and generates an HMAC SHA256 signature using the provided secret
        key. The signature and the updated parameters are then returned.

        :param params: Dictionary of request parameters to be signed
            (default is empty).
        :return: A tuple containing the updated parameters with a
            timestamp and a dictionary with the generated signature.
        """
        data = params.copy()
        ts = str(int(1000 * time.time()))
        data.update({"timestamp": ts})
        h = urlencode(data)
        h = h.replace("%40", "@")
        b = bytearray()
        b.extend(self.secret_key.encode())
        signature = hmac.new(
            b, msg=h.encode("utf-8"), digestmod=hashlib.sha256
        ).hexdigest()
        sig = {"signature": signature}
        return data, sig

    def _get(self, path, params):
        """
        Make a GET request to the API with signed parameters.

        This function signs the given parameters, constructs the request
        URL with the signature, and makes a GET request to the specified
        path. The response from the GET request is returned.

        :param path: The API endpoint path.
        :param params: Dictionary of request parameters to be signed and
            sent.
        :return: Response object from the GET request.
        """
        sign = self._sign(params)
        query = urlencode(sign[0]) + "&" + urlencode(sign[1])
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.api_key}
        resultGetFunction = requests.get(
            url, headers=header, timeout=30, verify=True
        )
        return resultGetFunction
