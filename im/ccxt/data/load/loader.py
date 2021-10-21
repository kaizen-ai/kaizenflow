"""
Import as:

import im.ccxt.data.load.loader as imccdaloloa
"""

import logging
import os
from typing import Optional

import pandas as pd

import core.pandas_helpers as cpah
import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# Path to the data about downloaded currencies from the spreadsheet in CMTask41.
_DOWNLOADED_CURRENCIES_PATH = "im/data/downloaded_currencies.json"


def _get_file_path(
    data_snapshot: str,
    exchange_id: str,
    currency_pair: str,
) -> str:
    """
    Get name for a file with CCXT data.

    File path is constructed in the following way:
    `ccxt/<snapshot>/<exchange_id>/<currency_pair>.csv.gz`.

    :param data_snapshot: snapshot of datetime when data was loaded, e.g. "20210924"
    :param exchange_id: CCXT exchange id, e.g. "binance"
    :param currency_pair: currency pair `<currency1>/<currency2>`, e.g. "BTC/USDT"
    :return: path to a file with CCXT data
    """
    # Extract data about downloaded currencies for CCXT.
    downloaded_currencies_info = hio.from_json(_DOWNLOADED_CURRENCIES_PATH)["CCXT"]
    # Verify that data for the input exchange id was downloaded.
    dbg.dassert_in(
        exchange_id,
        downloaded_currencies_info.keys(),
        msg="Data for exchange id='%s' was not downloaded" % exchange_id,
    )
    # Verify that data for the input exchange id and currency pair was
    # downloaded.
    downloaded_currencies = downloaded_currencies_info[exchange_id]
    dbg.dassert_in(
        currency_pair,
        downloaded_currencies,
        msg="Data for exchange id='%s', currency pair='%s' was not downloaded"
        % (exchange_id, currency_pair),
    )
    file_path = f"ccxt/{data_snapshot}/{exchange_id}/{currency_pair.replace('/', '_')}.csv.gz"
    return file_path


class CcxtLoader:
    def __init__(self, root_dir: str, aws_profile: Optional[str] = None) -> None:
        """
        Load CCXT data from different backends, e.g., DB, local or S3
        filesystem.

        :param: root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path ("s3://alphamatic-data/data") to the CCXT data
        :param: aws_profile: AWS profile name (e.g., "am")
        """
        self._root_dir = root_dir
        self._aws_profile = aws_profile
        # Specify supported data types to load.
        self._data_types = ["ohlcv"]

    def read_data(
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
        # Verify that requested data type is valid.
        hdbg.dassert_in(data_type.lower(), self._data_types)
        # Get absolute file path for a CCXT file.
        file_path = self._get_file_path(data_snapshot, exchange_id, currency_pair)
        # Initialize kwargs dict for further CCXT data reading.
        read_csv_kwargs = {}
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists and fill kwargs if needed.
        if hs3.is_s3_path(file_path):
            s3fs = hs3.get_s3fs(self._aws_profile)
            hs3.dassert_s3_exists(file_path, s3fs)
            # Add s3fs argument to kwargs.
            read_csv_kwargs["s3fs"] = s3fs
        else:
            hdbg.dassert_file_exists(file_path)
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
        # Extract data about downloaded currencies for CCXT.
        downloaded_currencies_info = hio.from_json(_DOWNLOADED_CURRENCIES_PATH)[
            "CCXT"
        ]
        # Verify that data for the input exchange id was downloaded.
        hdbg.dassert_in(
            exchange_id,
            downloaded_currencies_info.keys(),
            msg="Data for exchange id='%s' was not downloaded" % exchange_id,
        )
        # Verify that data for the input exchange id and currency pair was
        # downloaded.
        downloaded_currencies = downloaded_currencies_info[exchange_id]
        hdbg.dassert_in(
            currency_pair,
            downloaded_currencies,
            msg="Data for exchange id='%s', currency pair='%s' was not downloaded"
            % (exchange_id, currency_pair),
        )
        # Get absolute file path.
        file_name = currency_pair.replace("/", "_") + ".csv.gz"
        file_path = os.path.join(
            self._root_dir, "ccxt", data_snapshot, exchange_id, file_name
        )
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
        # Set timestamp as index.
        data = data.set_index("timestamp")
        # Add columns with exchange id and currency pair.
        data["exchange_id"] = exchange_id
        data["currency_pair"] = currency_pair
        return data

    @staticmethod
    def _convert_epochs_to_timestamp(epoch_col: pd.Series) -> pd.Series:
        """
        Convert Unix epoch to timestamp in a specified timezone.

        All Unix time epochs in CCXT are provided in ms and in UTC tz.

        :param epoch_col: Series with Unix time epochs
        :param timezone: "ET" or "UTC"
        :return: Series with epochs converted to timestamps
        """
        # Set tz value and verify that it is valid.
        tz = tz or "ET"
        dbg.dassert_in(tz, ["ET", "UTC"])
        # Convert to timestamp in UTC tz.
        timestamp_col = pd.to_datetime(epoch_col, unit="ms", utc=True)
        # Convert to ET tz if specified.
        if tz == "ET":
            timestamp_col = timestamp_col.dt.tz_convert(hdatet.get_ET_tz())
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
