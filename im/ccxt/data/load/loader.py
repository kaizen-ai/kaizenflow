import logging
import os

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)


def get_file_name(exchange: str, currency: str) -> str:
    """
    Get name for a file with CCXT data.

    File name is constructed in the following way:
    "<exchange>_<currency1>_<currency2>.csv.gz.

    :param exchange: CCXT exchange id
    :param currency: currency pair "<currency1>/<currency2>" (e.g. "BTC/USDT")
    :return: name for a file with CCXT data
    """
    file_name = f"{exchange}_{currency.replace('/', '_')}.csv.gz"
    return file_name


class CcxtLoader:
    """
    Load CCXT data.
    """

    def read_data(
        self, exchange: str, currency: str, data_type: str
    ) -> pd.DataFrame:
        """
        Load data from s3 and process it.

        :param exchange: CCXT exchange id
        :param currency: currency pair (e.g. "BTC/USDT")
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed CCXT data
        """
        # Get file path for a CCXT file.
        file_name = get_file_name(exchange, currency)
        s3_bucket = hs3.get_path()
        file_path = os.path.join(s3_bucket, file_name)
        # Read raw CCXT data from s3.
        _LOG.info("Reading file '%s'...", file_path)
        data = pd.read_csv(file_path)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CCXT data for exchange='%s', currencies='%s'...",
            exchange,
            currency,
        )
        transformed_data = self._transform(
            data, exchange, currency, data_type
        )
        return transformed_data

    def _transform(
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
            transformed_data = self._apply_ohlcv_transformation(transformed_data)
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
