import logging

import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class CcxtTransformer:
    """
    Class to transform raw CCXT data into DB view.
    """
    # TODO(*): To merge into single `Loader` class as separate method.
    #  Remove this class afterwards.
    def transform(
        self, data: pd.DataFrame, exchange: str, currency: str, data_type: str
    ):
        """
        Transform CCXT data loaded from S3.

        :param data: dataframe with CCXT data from S3
        :param exchange: CCXT exchange id
        :param currency: currency pair (e.g. "BTC/USDT")
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed dataframe
        """
        transformed_data = self._apply_ccxt_transformation(
            data, exchange, currency
        )
        if data_type.lower() == "ohlcv":
            transformed_data = cls._apply_ohlcv_transformation(transformed_data)
        else:
            dbg.dfatal("Incorrect data type. Acceptable types: ohlcv")
        return transformed_data

    @staticmethod
    def _apply_ccxt_transformation(
        data: pd.DataFrame, exchange: str, currency: str
    ):
        """
        Apply transform common to all CCXT data.

        This includes:
        - datetime format assertion
        - Converting epoch ms timestamp to pd.Timestamp
        - Adding exchange_id and currency_pair columns
        :return:
        """
        return transformed_data

    @staticmethod
    def _apply_ohlcv_transformation(transformed_data: pd.DataFrame):
        """
        Apply transformations for OHLCV data.

        This includes:
        - Assertion of present columns
        - Assertion of data types
        - Renaming and rearranging of OHLCV columns, namely:
            ["timestamp",
             "open",
             "high",
             "low",
             "close"
             "volume",
             "epoch",
             "currency_pair",
             "exchange"]
        :return:
        """
        return transformed_ohlcv
