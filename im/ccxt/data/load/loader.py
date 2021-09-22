import logging
import os

import pandas as pd

import ccxt
import core.pandas_helpers as pdhelp
import helpers.datetime_ as hdatet
import helpers.dbg as dbg
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# List from the spreadsheet:
# https://docs.google.com/spreadsheets/d/1qIw4AvPr3Ykh5zlRsNNEVzzPuyq-F3JMh_UZQS0kRhA/edit#gid=0
_DOWNLOADED_EXCHANGES_CURRENCIES = {
    "binance": [
        "ADA/USDT",
        "AVAX/USDT",
        "BNB/USDT",
        "BTC/USDT",
        "DOGE/USDT",
        "EOS/USDT",
        "ETH/USDT",
        "LINK/USDT",
        "SOL/USDT",
    ],
    "kucoin": [
        "ADA/USDT",
        "AVAX/USDT",
        "BNB/USDT",
        "BTC/USDT",
        "DOGE/USDT",
        "EOS/USDT",
        "ETH/USDT",
        "FIL/USDT",
        "LINK/USDT",
        "SOL/USDT",
        "XPR/USDT",
    ],
}


def _get_file_name(exchange: str, currency: str) -> str:
    """
    Get name for a file with CCXT data.

    File name is constructed in the following way:
    `<exchange>_<currency1>_<currency2>.csv.gz`.

    :param exchange: CCXT exchange id (e.g. "binance")
    :param currency: currency pair `<currency1>/<currency2>` (e.g. "BTC/USDT")
    :return: name for a file with CCXT data
    """
    # Make sure that data for the input exchange was downloaded.
    dbg.dassert_in(
        exchange,
        _DOWNLOADED_EXCHANGES_CURRENCIES.keys(),
        msg="Data for exchange='%s' was not downloaded" % exchange,
    )
    # Make sure that data for the input exchange, currency was downloaded.
    downloaded_currencies = _DOWNLOADED_EXCHANGES_CURRENCIES[exchange]
    dbg.dassert_in(
        currency,
        downloaded_currencies,
        msg="Data for exchange='%s', currency pair='%s' was not downloaded"
            % (exchange, currency),
    )
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
        Load data from S3 and process it in the common format used by the
        models.

        :param exchange: CCXT exchange id
        :param currency: currency pair (e.g. "BTC/USDT")
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed CCXT data
        """
        # Get file path for a CCXT file.
        file_name = _get_file_name(exchange, currency)
        s3_bucket_path = hs3.get_path()
        file_path = os.path.join(s3_bucket_path, file_name)
        # Make sure that the file exists.
        s3fs = hs3.get_s3fs("am")
        hs3.dassert_s3_exists(file_path, s3fs)
        # Read raw CCXT data from S3.
        _LOG.info(
            "Reading CCXT data for exchange='%s', currencies='%s' from file='%s'...",
            exchange,
            currency,
            file_path,
        )
        data = pdhelp.read_csv(file_path, s3fs)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CCXT data for exchange='%s', currencies='%s'...",
            exchange,
            currency,
        )
        transformed_data = self._transform(data, exchange, currency, data_type)
        return transformed_data

    # TODO(*): Consider making `exchange_id` a class member.
    def _transform(
        self, data: pd.DataFrame, exchange_id: str, currency_pair: str, data_type: str
    ) -> pd.DataFrame:
        """
        Transform CCXT data loaded from S3.

        Input data example:
            timestamp      open     high     low      close    volume
            1631145600000  3499.01  3499.49  3496.17  3496.36  346.4812
            1631145660000  3496.36  3501.59  3495.69  3501.59  401.9576
            1631145720000  3501.59  3513.10  3499.89  3513.09  579.5656

        Output data example:
            timestamp                  open     high     low      close    volume    epoch          currency_pair exchange_id
            2021-09-09 00:00:00+00:00  3499.01  3499.49  3496.17  3496.36  346.4812  1631145600000  BTC/USDT      binance
            2021-09-09 00:01:00+00:00  3496.36  3501.59  3495.69  3501.59  401.9576  1631145660000  BTC/USDT      binance
            2021-09-09 00:02:00+00:00  3501.59  3513.10  3499.89  3513.09  579.5656  1631145720000  BTC/USDT      binance

        :param data: dataframe with CCXT data from S3
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed dataframe
        """
        transformed_data = self._apply_common_transformation(
            data, exchange_id, currency_pair
        )
        if data_type.lower() == "ohlcv":
            transformed_data = self._apply_ohlcv_transformation(transformed_data)
        else:
            dbg.dfatal("Incorrect data type. Acceptable types: ohlcv")
        return transformed_data

    def _apply_common_transformation(
        self, data: pd.DataFrame, exchange_id: str, currency_pair: str
    ) -> pd.DataFrame:
        """
        Apply transform common to all CCXT data.

        This includes:
        - Datetime format assertion
        - Converting epoch ms timestamp to pd.Timestamp
        - Adding exchange_id and currency_pair columns

        :param data: raw data from S3
        :param exchange_id: name of exchange, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :return: transformed CCXT data
        """
        # Verify that the timestamp data is provided in ms.
        dbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        # Rename col with original Unix ms epoch.
        data = data.rename({"timestamp": "epoch"}, axis=1)
        # Transform Unix epoch into standard timestamp.
        data["timestamp"] = self._convert_epochs_to_timestamp(
            data["epoch"], exchange_id
        )
        # Add columns with exchange id and currency pair.
        data["exchange_id"] = exchange_id
        data["currency_pair"] = currency_pair
        return data

    @staticmethod
    def _convert_epochs_to_timestamp(
        epoch_col: pd.Series, exchange_id: str
    ) -> pd.Series:
        """
        Convert Unix epoch to timestamp.

        All timestamps in CCXT are provided with UTC tz.

        :param epoch_col: Series with unix time epochs
        :param exchange_id: name of exchange
        :return: Series with epochs converted to UTC timestamps
        """
        exchange_class = getattr(ccxt, exchange_id)
        timestamp_col = epoch_col.apply(exchange_class.iso8601)
        # Convert to timestamp.
        timestamp_col = hdatet.to_generalized_datetime(timestamp_col)
        return timestamp_col

    @staticmethod
    def _apply_ohlcv_transformation(data: pd.DataFrame) -> pd.DataFrame:
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
             "exchange_id"]

        :param data: data after general CCXT transforms
        :return: transformed OHLCV dataframe
        """
        ohlcv_columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "epoch",
            "currency_pair",
            "exchange_id",
        ]
        # Verify that dataframe contains OHLCV columns.
        dbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data[ohlcv_columns].copy()
        return data
