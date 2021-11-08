"""
Import as:

import im.ccxt.data.load.loader as imccdaloloa
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

import core.pandas_helpers as cpah
import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.hpandas as hhpandas
import helpers.s3 as hs3
import helpers.sql as hsql
import im.data.universe as imdauni

_LOG = logging.getLogger(__name__)

# Latest historical data snapshot.
_LATEST_DATA_SNAPSHOT = "20210924"


class CcxtLoader:
    def __init__(
        self,
        connection: Optional[hsql.DbConnection] = None,
        root_dir: Optional[str] = None,
        aws_profile: Optional[str] = None,
        remove_dups: bool = True,
        resample_to_1_min: bool = True,
    ) -> None:
        """
        Load CCXT data from different backends, e.g., DB, local or S3
        filesystem.

        :param connection: connection for a SQL database
        :param: root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path ("s3://alphamatic-data/data") to the CCXT data
        :param: aws_profile: AWS profile name (e.g., "am")
        :param remove_dups: whether to remove full duplicates or not
        :param resample_to_1_min: whether to resample to 1 min or not
        """
        self._connection = connection
        self._root_dir = root_dir
        self._aws_profile = aws_profile
        self._remove_dups = remove_dups
        self._resample_to_1_min = resample_to_1_min
        if self._aws_profile:
            self._s3fs = hs3.get_s3fs(self._aws_profile)
        # Specify supported data types to load.
        self._data_types = ["ohlcv"]

    def read_data_from_db(
        self,
        table_name: str,
        exchange_ids: Optional[Tuple[str]] = None,
        currency_pairs: Optional[Tuple[str]] = None,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        **read_sql_kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Load CCXT data from database.

        This code path is used for the real-time data.

        :param table_name: name of the table to load, e.g., "ccxt_ohlcv"
        :param exchange_ids: exchange ids to load data for
        :param currency_pairs: currency pairs to load data for
        :param start_date: the earliest data to load data for as unix epoch,
            e.g., `pd.Timestamp('2021-09-09')`
        :param end_date: the latest date to load data for as unix epoch,
            e.g., `pd.Timestamp('2021-09-09')`
        :param read_sql_kwargs: kwargs for `pd.read_sql()` query
        :return: table from database
        """
        # Verify that DB connection is provided.
        hdbg.dassert_is_not(self._connection, None)
        # Verify that table with specified name exists.
        hdbg.dassert_in(table_name, hsql.get_table_names(self._connection))
        # Initialize SQL query.
        sql_query = "SELECT * FROM %s" % table_name
        # Initialize lists for query condition strings and parameters to insert.
        query_conditions = []
        query_params = []
        # For every conditional parameter if it is provided, append
        # a corresponding query string to the query conditions list and
        # the corresponding parameter to the query parameters list.
        if exchange_ids:
            query_conditions.append("exchange_id IN %s")
            query_params.append(exchange_ids)
        if currency_pairs:
            query_conditions.append("currency_pair IN %s")
            query_params.append(currency_pairs)
        if start_date:
            start_date = hdatetim.convert_timestamp_to_unix_epoch(start_date)
            query_conditions.append("timestamp > %s")
            query_params.append(start_date)
        if end_date:
            end_date = hdatetim.convert_timestamp_to_unix_epoch(end_date)
            query_conditions.append("timestamp < %s")
            query_params.append(end_date)
        if query_conditions:
            # Append all the provided query conditions to the main SQL query.
            query_conditions = " AND ".join(query_conditions)
            sql_query = " WHERE ".join([sql_query, query_conditions])
        # Add a tuple of gathered query parameters to kwargs as `params`.
        read_sql_kwargs["params"] = tuple(query_params)
        # Execute SQL query.
        table = pd.read_sql(sql_query, self._connection, **read_sql_kwargs)
        return table

    def read_universe_data_from_filesystem(
        self,
        universe: Union[str, List[imdauni.ExchangeCurrencyTuple]],
        data_type: str,
        data_snapshot: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load data from S3 for specified universe.

        Output data is indexed by timestamp and contains the columns open,
        high, low, close, volume, epoch, currency_pair, exchange_id, e.g.,
        ```
        timestamp                  open        epoch          currency_pair exchange_id
        2018-08-16 20:00:00-04:00  6316.01 ... 1534464000000  BTC/USDT      binance
        2018-08-16 20:01:00-04:00  6311.36     1534464060000  BTC/USDT      binance
        ...
        2021-09-08 20:00:00-04:00  1.10343     1631145600000  XRP/USDT      kucoin
        2021-09-08 20:02:00-04:00  1.10292     1631145720000  XRP/USDT      kucoin
        ```

        :param universe: CCXT universe version or a list of exchange-currency
            tuples to load data for
        :param data_type: OHLCV or trade, bid/ask data
        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :return: processed CCXT data
        """
        # Load all the corresponding exchange-currency tuples if a universe
        # version is provided.
        if isinstance(universe, str):
            universe = imdauni.get_vendor_universe_as_tuples(universe, "CCXT")
        # Initialize results df.
        combined_data = pd.DataFrame(dtype="object")
        # Load data for each exchange-currency tuple and append to results df.
        for exchange_currency_tuple in universe:
            data = self.read_data_from_filesystem(
                exchange_currency_tuple.exchange_id,
                exchange_currency_tuple.currency_pair,
                data_type,
                data_snapshot,
            )
            combined_data = combined_data.append(data)
        # Sort results by exchange id and currency pair.
        combined_data.sort_values(
            by=["exchange_id", "currency_pair"], inplace=True
        )
        return combined_data

    def read_data_from_filesystem(
        self,
        exchange_id: str,
        currency_pair: str,
        data_type: str,
        data_snapshot: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load data from S3 and process it for use downstream.

        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :param data_type: OHLCV or trade, bid/ask data
        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :return: processed CCXT data
        """
        data_snapshot = data_snapshot or _LATEST_DATA_SNAPSHOT
        # Verify that root dir is provided.
        hdbg.dassert_is_not(self._root_dir, None)
        # Verify that requested data type is valid.
        hdbg.dassert_in(data_type.lower(), self._data_types)
        # Get absolute file path for a CCXT file.
        file_path = self._get_file_path(data_snapshot, exchange_id, currency_pair)
        # Initialize kwargs dict for further CCXT data reading.
        read_csv_kwargs = {}
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            read_csv_kwargs["s3fs"] = self._s3fs
        # Read raw CCXT data.
        _LOG.info(
            "Reading CCXT data for exchange id='%s', currencies='%s' from file='%s'...",
            exchange_id,
            currency_pair,
            file_path,
        )
        data = cpah.read_csv(file_path, **read_csv_kwargs)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CCXT data for exchange id='%s', currencies='%s'...",
            exchange_id,
            currency_pair,
        )
        transformed_data = self._transform(
            data, exchange_id, currency_pair, data_type
        )
        return transformed_data

    # TODO(Grisha): factor out common code from `CddLoader._get_file_path` and `CcxtLoader._get_file_path`.
    def _get_file_path(
        self,
        data_snapshot: str,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with CCXT data.

        The file path is constructed in the following way:
        `<root_dir>/ccxt/<snapshot>/<exchange_id>/<currency_pair>.csv.gz`.

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>/<currency2>`,
            e.g. "BTC/USDT"
        :return: absolute path to a file with CCXT data
        """
        # Get absolute file path.
        file_name = currency_pair.replace("/", "_") + ".csv.gz"
        file_path = os.path.join(
            self._root_dir, "ccxt", data_snapshot, exchange_id, file_name
        )
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists.
        if hs3.is_s3_path(file_path):
            hs3.dassert_s3_exists(file_path, self._s3fs)
        else:
            hdbg.dassert_file_exists(file_path)
        return file_path

    # TODO(*): Consider making `exchange_id` a class member.
    def _transform(
        self,
        data: pd.DataFrame,
        exchange_id: str,
        currency_pair: str,
        data_type: str,
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
            2021-09-08 20:00:00-04:00  3499.01  3499.49  3496.17  3496.36  346.4812  1631145600000  ETH/USDT      binance
            2021-09-08 20:01:00-04:00  3496.36  3501.59  3495.69  3501.59  401.9576  1631145660000  ETH/USDT      binance
            2021-09-08 20:02:00-04:00  3501.59  3513.10  3499.89  3513.09  579.5656  1631145720000  ETH/USDT      binance

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
            hdbg.dfatal(
                "Incorrect data type: '%s'. Acceptable types: '%s'"
                % (data_type.lower(), self._data_types)
            )
        return transformed_data

    def _apply_common_transformation(
        self, data: pd.DataFrame, exchange_id: str, currency_pair: str
    ) -> pd.DataFrame:
        """
        Apply transform common to all CCXT data.

        This includes:
        - Datetime format assertion
        - Converting epoch ms timestamp to pd.Timestamp
        - Removing full duplicates
        - Resampling to 1 minute using NaNs
        - Adding exchange_id and currency_pair columns

        :param data: raw data from S3
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :return: transformed CCXT data
        """
        # Verify that the timestamp data is provided in ms.
        hdbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        # Rename col with original Unix ms epoch.
        data = data.rename({"timestamp": "epoch"}, axis=1)
        # Transform Unix epoch into ET timestamp.
        data["timestamp"] = self._convert_epochs_to_timestamp(data["epoch"])
        #
        if self._remove_dups:
            # Remove full duplicates.
            data = hhpandas.drop_duplicates(data, ignore_index=True)
        # Set timestamp as index.
        data = data.set_index("timestamp")
        #
        if self._resample_to_1_min:
            # Resample to 1 minute.
            data = hhpandas.resample_df(data, "T")
        # Add columns with exchange id and currency pair.
        data["exchange_id"] = exchange_id
        data["currency_pair"] = currency_pair
        return data

    @staticmethod
    def _convert_epochs_to_timestamp(epoch_col: pd.Series) -> pd.Series:
        """
        Convert Unix epoch to timestamp in ET.

        All Unix time epochs in CCXT are provided in ms and in UTC tz.

        :param epoch_col: Series with Unix time epochs
        :return: Series with epochs converted to timestamps in ET
        """
        # Convert to timestamp in UTC tz.
        timestamp_col = pd.to_datetime(epoch_col, unit="ms", utc=True)
        # Convert to ET tz.
        timestamp_col = timestamp_col.dt.tz_convert(hdatetim.get_ET_tz())
        return timestamp_col

    @staticmethod
    def _apply_ohlcv_transformation(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations for OHLCV data.

        This includes:
        - Assertion of present columns
        - Assertion of data types
        - Renaming and rearranging of OHLCV columns, namely:
            ["open",
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
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data[ohlcv_columns].copy()
        return data
