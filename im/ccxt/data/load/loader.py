import logging

import pandas as pd

import ccxt
import helpers.datetime_ as hdatet
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class CcxtLoader:
    """
    Load CCXT data.
    """

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
        - Rearranging of OHLCV columns, namely:
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
