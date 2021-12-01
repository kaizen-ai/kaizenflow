"""
Import as:

import im.cryptodatadownload.data.load.loader as icdalolo
"""

import logging
import os
from typing import Optional

import pandas as pd

import core.pandas_helpers as cpanh
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# Latest historical data snapsot.
_LATEST_DATA_SNAPSHOT = "20210924"


class CddLoader:
    def __init__(
        self,
        root_dir: str,
        aws_profile: Optional[str] = None,
        remove_dups: bool = True,
        resample_to_1_min: bool = True,
    ) -> None:
        """
        Load CDD data.

        :param: root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path ("s3://alphamatic-data/data) to CDD data
        :param: aws_profile: AWS profile name (e.g., "am")
        :param remove_dups: whether to remove full duplicates or not
        :param resample_to_1_min: whether to resample to 1 min or not
        """
        self._root_dir = root_dir
        self._aws_profile = aws_profile
        self._remove_dups = remove_dups
        self._resample_to_1_min = resample_to_1_min
        self._s3fs = hs3.get_s3fs(self._aws_profile)
        # Specify supported data types to load.
        self._data_types = ["ohlcv"]

    def read_data_from_filesystem(
        self,
        exchange_id: str,
        currency_pair: str,
        data_type: str,
        data_snapshot: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load data from S3 and process it for use downstream.

        :param exchange_id: CDD exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :param data_type: OHLCV or trade, bid/ask data
        :param data_snapshot: snapshot of datetime when data was loaded, e.g. "20210924"
        :return: processed CDD data
        """
        data_snapshot = data_snapshot or _LATEST_DATA_SNAPSHOT
        # Verify that requested data type is valid.
        hdbg.dassert_in(
            data_type.lower(),
            self._data_types,
            msg="Incorrect data type: '%s'. Acceptable types: '%s'"
            % (data_type.lower(), self._data_types),
        )
        # Get absolute file path for a CDD file.
        file_path = self._get_file_path(data_snapshot, exchange_id, currency_pair)
        # Initialize kwargs dict for further CDD data reading.
        # Add "skiprows" to kwargs in order to skip a row with the file name.
        read_csv_kwargs = {"skiprows": 1}
        #
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            read_csv_kwargs["s3fs"] = self._s3fs
        # Read raw CDD data.
        _LOG.info(
            "Reading CDD data for exchange id='%s', currencies='%s', from file='%s'...",
            exchange_id,
            currency_pair,
            file_path,
        )
        data = cpanh.read_csv(file_path, **read_csv_kwargs)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CDD data for exchange id='%s', currencies='%s'...",
            exchange_id,
            currency_pair,
        )
        transformed_data = self._transform(
            data, exchange_id, currency_pair, data_type
        )
        return transformed_data

    # TODO(Grisha): factor out common code from `CddLoader._get_file_path` and
    # `CcxtLoader._get_file_path`.
    def _get_file_path(
        self,
        data_snapshot: str,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with CDD data.

        The file path is constructed in the following way:
        `<root_dir>/cryptodatadownload/<snapshot>/<exchange_id>/<currency_pair>.csv.gz`.

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: CDD exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>/<currency2>`,
            e.g. "BTC/USDT"
        :return: absolute path to a file with CDD data
        """
        # Get absolute file path.
        file_name = currency_pair.replace("/", "_") + ".csv.gz"
        file_path = os.path.join(
            self._root_dir,
            "cryptodatadownload",
            data_snapshot,
            exchange_id,
            file_name,
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
        Transform CDD data loaded from S3.

        Input data example:
        ```
        unix           date                 symbol    open     high     low      close    Volume ETH  Volume USDT  tradecount
        1631145600000  2021-09-09 00:00:00  ETH/USDT  3499.01  3499.49  3496.17  3496.36  346.4812    1212024      719
        1631145660000  2021-09-09 00:01:00  ETH/USDT  3496.36  3501.59  3495.69  3501.59  401.9576    1406241      702
        1631145720000  2021-09-09 00:02:00  ETH/USDT  3501.59  3513.10  3499.89  3513.09  579.5656    2032108      1118
        ```

        Output data example:
        ```
        timestamp                  open     high     low      close    volume    epoch          currency_pair exchange_id
        2021-09-08 20:00:00-04:00  3499.01  3499.49  3496.17  3496.36  346.4812  1631145600000  ETH/USDT      binance
        2021-09-08 20:01:00-04:00  3496.36  3501.59  3495.69  3501.59  401.9576  1631145660000  ETH/USDT      binance
        2021-09-08 20:02:00-04:00  3501.59  3513.10  3499.89  3513.09  579.5656  1631145720000  ETH/USDT      binance
        ```

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
            hdbg.dfatal(
                "Incorrect data type: '%s'. Acceptable types: '%s'"
                % (data_type.lower(), self._data_types)
            )
        return transformed_data

    def _apply_common_transformation(
        self, data: pd.DataFrame, exchange_id: str, currency_pair: str
    ) -> pd.DataFrame:
        """
        Apply transform common to all CDD data.

        This includes:
        - Datetime format assertion
        - Converting string dates to UTC `pd.Timestamp`
        - Removing full duplicates
        - Resampling to 1 minute using NaNs
        - Name volume and currency pair columns properly
        - Adding exchange_id and currency_pair columns

        :param data: raw data from S3
        :param exchange_id: CDD exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :return: transformed CDD data
        """
        # Verify that the Unix data is provided in ms.
        hdbg.dassert_container_type(
            data["unix"], container_type=None, elem_type=int
        )
        # Rename col with original Unix ms epoch.
        data = data.rename({"unix": "epoch"}, axis=1)
        # Transform Unix epoch into UTC timestamp.
        data["timestamp"] = pd.to_datetime(data["epoch"], unit="ms", utc=True)
        #
        if self._remove_dups:
            # Remove full duplicates.
            data = hpandas.drop_duplicates(data, ignore_index=True)
        # Set timestamp as index.
        data = data.set_index("timestamp")
        #
        if self._resample_to_1_min:
            # Resample to 1 minute.
            data = hpandas.resample_df(data, "T")
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
