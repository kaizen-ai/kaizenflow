"""
Import as:

import im_v2.ccxt.data.extract.extractor as ivcdexkex
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
import im_v2.ccxt.data.extract.extractor as ivcdexex
import im_v2.common.data.extract.extractor as imvcdexex

_LOG = logging.getLogger(__name__)

class CcxtKrakenExtractor(ivcdexex.CcxtExtractor):
    def __init__(self, exchange_id: str, contract_type: str) -> None:
        # Stick to Kraken futures.
        hdbg.dassert_eq(exchange_id, "kraken")
        hdbg.dassert_eq(contract_type, "futures")
        super().__init__()

    def log_into_exchange(
        self, async_: bool
    ) -> ccxt.Exchange:
        """
        Log into an Kraken via CCXT and return the
        corresponding `Exchange` object.

        :param async_: leave for compatibility with other exchanges
        """
        return ccxt.krakenfutures()

    @staticmethod
    def convert_currency_pair(
        currency_pair: str, *, exchange_id: str = None
    ) -> str:
        """
        Convert currency pair used for getting data from exchange.
        """
         # Stick to Kraken futures.
        hdbg.dassert_eq(exchange_id, "kraken")
        # Kraken futures uses a different naming convention for currency pairs.
        # For example, `BTC_USDT` is `BTC/USD:USD`.
        return currency_pair.split("/")[0]
