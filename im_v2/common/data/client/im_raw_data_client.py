"""
Thin IM client for loading raw data.

Import as:

import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
"""

import logging
from typing import List, Optional

import pandas as pd

import core.finance.bid_ask as cfibiask
import data_schema.dataset_schema_utils as dsdascut
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hsql_implementation as hsqlimpl
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)

# TODO(Juraj): This might be moved to helpers.
_S3_BUCKET_BY_STAGE = {
    "test": "cryptokaizen-data-test",
    "preprod": "cryptokaizen-data.preprod",
    "prod": "cryptokaizen-data",
}


class RawDataReader:
    """
    Load the raw data sample from S3 or the DB.
    """

    def __init__(self, signature: str, *, stage: str = "prod"):
        """
        Constructor.

        :param signature: dataset signature,
          e.g. `bulk.airflow.resampled_1min.pq.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0`
        :param stage: which stage to execute in, determines which DB stage or S3 bucket is used.
        """
        # Validate signature schema.
        self.dataset_schema = dsdascut.get_dataset_schema()
        self.signature = signature
        self.args = dsdascut.parse_dataset_signature_to_args(
            signature, self.dataset_schema
        )
        self.stage = stage
        self.dataset_epoch_unit = imvcdttrut.get_vendor_epoch_unit(
            self.args["vendor"], self.args["data_type"]
        )
        if self.args["data_format"] == "postgres":
            self._setup_db_table_access()

    def read_data_head(self) -> pd.DataFrame:
        """
        Load the data sample.
        """
        if self.args["data_format"] == "parquet":
            # Load the data from Parquet.
            data = self.load_parquet_head()
        else:
            # Load the data from DB.
            data = self.load_db_table_head()
        return data

    def read_data(
        self,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        *,
        currency_pairs: Optional[List[str]] = None,
        bid_ask_levels: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        """
        Load data in a specified time interval.

        :param start_timestamp: start of the filtered interval
        :param end_timestamp: end of the filtered interval
        :param currency_pairs: currency_pairs to select, if None, all pairs are loaded
        :param bid_ask_levels: which levels of bid_ask data to load, if None,
          all levels are loaded
        """
        if self.args["data_format"] == "parquet":
            # Load the data from Parquet.
            data = self.load_parquet(
                start_timestamp,
                end_timestamp,
                currency_pairs=currency_pairs,
                bid_ask_levels=bid_ask_levels,
            )
        else:
            # Load the data from DB.
            data = self.load_db_table(
                start_timestamp,
                end_timestamp,
                currency_pairs=currency_pairs,
                bid_ask_levels=bid_ask_levels,
            )
        return data

    def load_parquet_head(self) -> pd.DataFrame:
        """
        Load the head of a sample parquet file.

        Currently using `currency_pair=ETH_USDT/year=2022/month=11/data.parquet` location for each data type.
        """
        # Build s3 path.
        s3_pq_file_path = self._build_s3_pq_file_path()
        _LOG.info("Loading the data from `%s`", s3_pq_file_path)
        df = hparque.from_parquet(s3_pq_file_path, n_rows=10, aws_profile="ck")
        return df

    def load_db_table_head(self) -> pd.DataFrame:
        """
        Load the head of the DB table corresponding to the dataset signature.
        """
        df = imvcddbut.load_db_data(
            self.db_connection, self.table_name, None, None, limit=10
        )
        # Remove useless id column.
        df = df.drop("id", axis=1)
        return df

    def load_db_table(
        self,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        *,
        currency_pairs: Optional[List[str]] = None,
        bid_ask_levels: Optional[List[int]] = None,
        bid_ask_format="wide",
    ) -> pd.DataFrame:
        """
        Load data from a DB table in a specified time interval.

        Refer to read_data() for parameter docs.
        """
        hdbg.dassert_in(bid_ask_format, ["wide", "long"])
        data = imvcddbut.load_db_data(
            self.db_connection,
            self.table_name,
            start_timestamp,
            end_timestamp,
            currency_pairs=currency_pairs,
            bid_ask_levels=bid_ask_levels,
        )
        data = data.drop_duplicates()
        if self.args["data_type"] == "bid_ask" and bid_ask_format == "wide":
            # Set timestamp as index as required by the transform function.
            data = data.set_index("timestamp", drop=True)
            data = cfibiask.transform_bid_ask_long_data_to_wide(data, "timestamp")
        return data

    def load_csv(
        self,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Load csv data in a specified time frame.

        :param currency_pair: currency pair to load
        :param start_timestamp: start of the filtered interval
        :param end_timestamp: end of the filtered interval
        :return: data frame
        """
        # Build s3 path.
        s3_csv_file_path = self._build_s3_csv_file_path(currency_pair)
        _LOG.info("Loading the data from `%s`", s3_csv_file_path)
        # Convert timestamps to unix epoch.
        start_timestamp = hdateti.convert_timestamp_to_unix_epoch(
            start_timestamp, "ms"
        )
        end_timestamp = hdateti.convert_timestamp_to_unix_epoch(
            end_timestamp, "ms"
        )
        # Load the data.
        df = pd.read_csv(s3_csv_file_path)
        # Filter the data.
        if start_timestamp:
            df = df[df["timestamp"] >= start_timestamp]
        if end_timestamp:
            df = df[df["timestamp"] <= end_timestamp]
        return df

    def load_parquet(
        self,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        *,
        currency_pairs: Optional[List[str]] = None,
        bid_ask_levels: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        """
        Load parquet data in a specified time frame.

        Refer to read_data() for parameter docs.
        """
        # TODO(Juraj): Add support once #3694 is finished.
        if bid_ask_levels is not None:
            raise ValueError("Level filtering not supported yet.")
        filters = []
        if start_timestamp:
            start_ts = hdateti.convert_timestamp_to_unix_epoch(
                start_timestamp, unit=self.dataset_epoch_unit
            )
            filters.append(("timestamp", ">=", start_ts))
        if end_timestamp:
            end_ts = hdateti.convert_timestamp_to_unix_epoch(
                end_timestamp, unit=self.dataset_epoch_unit
            )
            filters.append(("timestamp", "<=", end_ts))
        if currency_pairs:
            filters.append(("currency_pair", "in", currency_pairs))
        s3_path = self._build_s3_pq_file_path()
        data = hparque.from_parquet(s3_path, filters=filters, aws_profile="ck")
        return data

    def _setup_db_table_access(self) -> None:
        """
        Setup DB connection and get DB table name based on dataset signature.
        """
        self.db_connection = imvcddbut.DbConnectionManager.get_connection(
            self.stage
        )
        self.table_name = dsdascut.get_im_db_table_name_from_signature(
            self.signature, self.dataset_schema
        )
        # Check if the table name exists.
        db_tables = hsqlimpl.get_table_names(self.db_connection)
        hdbg.dassert_in(
            self.table_name, db_tables, f"`{self.table_name} doesn't exist`"
        )
        _LOG.info("Enabled connection to the `%s` DB table", self.table_name)

    def _build_s3_pq_file_path(self) -> str:
        """
        Get the path to Parquet file on S3.
        """
        hdbg.dassert_in(self.stage, _S3_BUCKET_BY_STAGE)
        s3_bucket = f"s3://{_S3_BUCKET_BY_STAGE[self.stage]}"
        s3_path = dsdascut.build_s3_dataset_path_from_args(s3_bucket, self.args)
        return s3_path

    def _build_s3_csv_file_path(self, currency_pair: str) -> str:
        """
        Get the path to CSV file on S3.

        :param currency_pair: currency pair to load
        :return: s3 path
        """
        hdbg.dassert_in(self.stage, _S3_BUCKET_BY_STAGE)
        s3_bucket = f"s3://{_S3_BUCKET_BY_STAGE[self.stage]}"
        print(s3_bucket)
        s3_path = dsdascut.build_s3_dataset_path_from_args(s3_bucket, self.args)
        print(s3_path)
        s3_path += f"/{currency_pair}.csv.gz"
        return s3_path
