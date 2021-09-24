"""
Import as:

import im.cryptodatadownload.data.load.loader as crdall
"""

import logging
import os

import pandas as pd

import core.pandas_helpers as cphelp
import helpers.datetime_ as hdatet
import helpers.dbg as dbg
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# TODO(Dan): Fill the lists and decide whether to put them outside of the file.
# Data about downloaded currencies from the spreadsheet in CMTask41.
_DOWNLOADED_EXCHANGES_TIMEFRAMES_CURRENCIES = {
    "binance": {
        "minute": [
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
    },
    "kucoin": {
        "minute": [
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
    },
}


def _get_file_name(exchange_id: str, currency_pair: str, timeframe: str) -> str:
    """
    Get name for a file with CDD data.

    File name is constructed in the following way:
    `<Exchange_id>_<currency1><currency2>_<timeframe>.csv.gz`.

    :param exchange_id: CDD exchange id, e.g. "binance"
    :param currency_pair: currency pair `<currency1>/<currency2>`, e.g. "BTC/USDT"
    :param timeframe: timeframe of the data to load. Possible values:
        'minute', 'hourly', 'daily'.
    :return: name for a file with CDD data
    """
    # Verify that data for the input exchange id was downloaded.
    dbg.dassert_in(
        exchange_id,
        _DOWNLOADED_EXCHANGES_TIMEFRAMES_CURRENCIES.keys(),
        msg="Data for exchange id='%s' was not downloaded" % exchange_id,
    )
    # Verify that data for the input exchange id and timeframe was
    # downloaded.
    downloaded_timeframes = _DOWNLOADED_EXCHANGES_TIMEFRAMES_CURRENCIES[
        exchange_id
    ]
    dbg.dassert_in(
        timeframe,
        downloaded_timeframes,
        msg="Data for exchange id='%s', timeframe='%s' was not downloaded"
        % (exchange_id, timeframe),
    )
    # Verify that data for the input exchange id, timeframe, and currency
    # pair was downloaded.
    downloaded_currencies = _DOWNLOADED_EXCHANGES_TIMEFRAMES_CURRENCIES[
        exchange_id
    ][timeframe]
    dbg.dassert_in(
        currency_pair,
        downloaded_currencies,
        msg="Data for exchange id='%s', timeframe='%s', currency pair='%s' was not downloaded"
        % (exchange_id, timeframe, currency_pair),
    )
    # TODO(Dan): Discuss unification of file names logic on S3 for CCXT and CDD.
    file_name = f"{exchange_id.capitalize()}_{currency_pair.replace('/', '')}_{timeframe}.csv.gz"
    return file_name


class CddLoader:
    """
    Load CDD data.
    """

    def read_data(
        self, exchange_id: str, currency_pair: str, timeframe: str, data_type: str
    ) -> pd.DataFrame:
        """
        Load data from S3 and process it for use downstream.

        :param exchange_id: CDD exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :param timeframe: timeframe of the data to load. Possible values:
            'minute', 'hourly', 'daily'.
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed CDD data
        """
        # Get file path for a CDD file.
        file_name = _get_file_name(exchange_id, currency_pair, timeframe)
        s3_bucket_path = hs3.get_path()
        file_path = os.path.join(s3_bucket_path, file_name)
        # Verify that the file exists.
        s3fs = hs3.get_s3fs("am")
        hs3.dassert_s3_exists(file_path, s3fs)
        # Read raw CDD data from S3.
        _LOG.info(
            "Reading CDD data for exchange id='%s', currencies='%s', timeframe='%s' from file='%s'...",
            exchange_id,
            currency_pair,
            timeframe,
            file_path,
        )
        data = cphelp.read_csv(file_path, s3fs)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CDD data for exchange id='%s', currencies='%s', timeframe='%s'...",
            exchange_id,
            currency_pair,
            timeframe,
        )
        transformed_data = self._transform(
            data, exchange_id, currency_pair, data_type
        )
        return transformed_data

    # TODO(*): Consider making `exchange_id` a class member.
    # TODO(Dan): Decide whether `timestamp` col should contain "+00:00" at the end (compare with CCXT).
    def _transform(
        self,
        data: pd.DataFrame,
        exchange_id: str,
        currency_pair: str,
        data_type: str,
    ) -> pd.DataFrame:
        """
        Transform CDD data loaded from S3.

        Input data example:
            unix           date                 symbol    open     high     low      close    Volume ETH  Volume USDT  tradecount
            1631145600000  2021-09-09 00:00:00  ETH/USDT  3499.01  3499.49  3496.17  3496.36  346.4812    1212024      719
            1631145660000  2021-09-09 00:01:00  ETH/USDT  3496.36  3501.59  3495.69  3501.59  401.9576    1406241      702
            1631145720000  2021-09-09 00:02:00  ETH/USDT  3501.59  3513.10  3499.89  3513.09  579.5656    2032108      1118

        Output data example:
            timestamp            open     high     low      close    volume    epoch          currency_pair exchange_id
            2021-09-09 00:00:00  3499.01  3499.49  3496.17  3496.36  346.4812  1631145600000  ETH/USDT      binance
            2021-09-09 00:01:00  3496.36  3501.59  3495.69  3501.59  401.9576  1631145660000  ETH/USDT      binance
            2021-09-09 00:02:00  3501.59  3513.10  3499.89  3513.09  579.5656  1631145720000  ETH/USDT      binance

        :param data: dataframe with CDD data from S3
        :param exchange_id: CDD exchange id, e.g. "binance"
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
        Apply transform common to all CDD data.

        This includes:
        - Datetime format assertion
        - Converting string dates to pd.Timestamp
        - Name volume and currency pair columns properly
        - Adding exchange_id and currency_pair columns

        :param data: raw data from S3
        :param exchange_id: CDD exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :return: transformed CDD data
        """
        # Verify that the Unix data is provided in ms.
        dbg.dassert_container_type(
            data["unix"], container_type=None, elem_type=int
        )
        # Rename col with original Unix ms epoch.
        data = data.rename({"unix": "epoch"}, axis=1)
        # Transform dates into standard timestamps.
        data["timestamp"] = hdatet.to_generalized_datetime(data["date"])
        # Rename col with traded volume in amount of the 1st currency in pair.
        data = data.rename(
            {"Volume " + currency_pair.split("/")[0]: "volume"}, axis=1
        )
        # Rename col with currency pair.
        data = data.rename({"symbol": "currency_pair"}, axis=1)
        # Add a col with exchange id.
        data["exchange_id"] = exchange_id
        return data

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

        :param data: data after general CDD transforms
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
