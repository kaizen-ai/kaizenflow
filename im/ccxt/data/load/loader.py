import logging

import ccxt
import pandas as pd
import helpers.datetime_ as hdatet
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class CcxtLoader:
    """
    Class to load CCXT data.
    """
    # TODO(*): To merge into `load_data` method.
    #  Remove this class afterwards.
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
        dbg.dassert_container_type(data["timestamp"], container_type=None, elem_type=int)
        transformed_data = data.copy()
        transformed_data = transformed_data.rename({"timestamp": "epoch"}, axis=1)
        transformed_data["timestamp"] = transformed_data["epoch"].apply(ccxt.)
        transformed_data["exchange"] = exchange
        transformed_data["currency_pair"] = currency
        return transformed_data

    @staticmethod
    def _convert_epochs_to_timestamp(epoch_col: pd.Series, exchange: str):
        exchange_class = getattr(ccxt, exchange)
        timestamp_col = epoch_col.apply(exchange_class.iso8601)
        # Convert to timestamp.
        timestamp_col = hdatet.to_generalized_datetime(timestamp_col)
        return timestamp_col

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
        ohlcv_columns = ["timestamp", "open", "high", "low", "close", "volume", "epoch", "currency_pair", "exchange"]
        transformed_ohlcv = transformed_data.copy()
        dbg.dassert_is_subset(transformed_ohlcv.columns, ohlcv_columns)
        return transformed_ohlcv
