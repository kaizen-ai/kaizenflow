import logging

import pandas as pd

import ccxt
import helpers.datetime_ as hdatet
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class CcxtLoader:
    """
    Class to load CCXT data.
    """
    def _transform(
        self, data: pd.DataFrame, exchange: str, currency: str, data_type: str
    ) -> pd.DataFrame:
        """
        Transform CCXT data loaded from S3.

        :param data: dataframe with CCXT data from S3
        :param exchange: CCXT exchange id, e.g. "binance"
        :param currency: currency pair, e.g. "BTC/USDT"
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

    def _apply_ccxt_transformation(
        self, data: pd.DataFrame, exchange: str, currency: str
    ) -> pd.DataFrame:
        """
        Apply transform common to all CCXT data.

        This includes:
        - datetime format assertion
        - Converting epoch ms timestamp to pd.Timestamp
        - Adding exchange_id and currency_pair columns

        :param data: raw data from S3
        :param exchange: name of exchange, e.g. "binance"
        :param currency: currency pair, e.g. "BTC/USDT"
        :return: transformed CCXT data
        """
        # Verify that the timestamp data is provided in ms.
        dbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        transformed_data = data.copy()
        # Rename col with original Unix ms epoch.
        transformed_data = transformed_data.rename({"timestamp": "epoch"}, axis=1)
        # Transform Unix epoch into standard timestamp.
        transformed_data["timestamp"] = self._convert_epochs_to_timestamp(
            transformed_data["epoch"], exchange
        )
        # Add columns with exchange and currency pair.
        transformed_data["exchange"] = exchange
        transformed_data["currency_pair"] = currency
        return transformed_data

    @staticmethod
    def _convert_epochs_to_timestamp(
        epoch_col: pd.Series, exchange: str
    ) -> pd.Series:
        """
        Convert Unix epoch to timestamp.

        All timestamps in CCXT are provided with UTC tz.

        :param epoch_col: Series with unix time epochs
        :param exchange: name of exchange
        :return: Series with epochs converted to UTC timestamps
        """
        exchange_class = getattr(ccxt, exchange)
        timestamp_col = epoch_col.apply(exchange_class.iso8601)
        # Convert to timestamp.
        timestamp_col = hdatet.to_generalized_datetime(timestamp_col)
        return timestamp_col

    @staticmethod
    def _apply_ohlcv_transformation(
        transformed_data: pd.DataFrame,
    ) -> pd.DataFrame:
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
             "exchange"]

        :param transformed_data: data after general CCXT transforms
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
            "exchange",
        ]
        transformed_ohlcv = transformed_data.copy()
        # Verify that dataframe contains OHLCV columns.
        dbg.dassert_is_subset(transformed_ohlcv.columns, ohlcv_columns)
        # Rearrange the columns.
        transformed_ohlcv = transformed_ohlcv[ohlcv_columns]
        return transformed_ohlcv
