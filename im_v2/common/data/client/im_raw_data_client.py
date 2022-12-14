"""
Thin IM client for loading raw data.

Import as:

import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
"""

import logging

import pandas as pd

import data_schema.dataset_schema_utils as dsdascut
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hsql as hsql
import helpers.hsql_implementation as hsqlimpl
import helpers.hdatetime as hdateti
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


class RawDataReader:
    """
    Load the raw data sample from S3 or the DB.
    """

    def __init__(self, signature: str):
        """
        Constructor.

        :param signature: dataset signature,
          e.g. `bulk.airflow.resampled_1min.pq.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0`
        """
        # Validate signature schema.
        dataset_schema = dsdascut.get_dataset_schema()
        self.args = dsdascut._parse_dataset_signature_to_args(
            signature, dataset_schema
        )

    def read_data(self) -> pd.DataFrame:
        """
        Load the data sample.
        """
        if self.args["data_format"] == "parquet":
            # Load the data from S3.
            data = self.load_parquet_head()
        else:
            # Load the data from DB.
            data = self.load_db_table_head()
        return data

    def load_parquet_head(self) -> pd.DataFrame:
        """
        Load the head of a sample parquet file.

        Currently using `currency_pair=ETH_USDT/year=2022/month=11/data.parquet` location for each data type.
        """
        # Build s3 path.
        s3_pq_file_path = self._build_s3_pq_file_path()
        _LOG.info(f"Loading the data from `{s3_pq_file_path}`")
        df = hparque.from_parquet(s3_pq_file_path, n_rows=10, aws_profile="ck")
        return df

    def load_db_table_head(self) -> pd.DataFrame:
        """
        Load the head of the DB table.
        """
        connection = imvcddbut.DbConnectionManager.get_connection("dev")
        table_name = self._get_db_table_name()
        # Check if the table name exists.
        db_tables = hsqlimpl.get_table_names(connection)
        hdbg.dassert_in(table_name, db_tables, f"`{table_name} doesn't exist`")
        # Load the head of the data.
        _LOG.info(f"Loading the data from `{table_name}` table")
        query_head = (
            f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT 10"
        )
        head = hsql.execute_query_to_df(connection, query_head)
        return head
    

    # TODO(Juraj): this is make-do solution, it needs consolidation with the rest of the code
    #  + adding docstrings.
    def load_parquet(
        self, s3_base_path: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Load parquet data in a specified time frame.
        """
        # TODO(Juraj): epoch unit is to second, which only works
        #  for crypto_chassis data.
        start_ts = hdateti.convert_timestamp_to_unix_epoch(
            start_ts, unit="s"
        )
        end_ts = hdateti.convert_timestamp_to_unix_epoch(
            end_ts, unit="s"
        )
        s3_path = dsdascut.build_s3_dataset_path_from_args(
            s3_base_path, self.args
        )
        filters = [("timestamp", ">=", start_ts), ("timestamp", "<=", end_ts)]
        data = hparque.from_parquet(
            s3_path, filters=filters, aws_profile="ck"
        )
        return data

    def _get_db_table_name(self) -> str:
        """
        Build the name of DB table according to the signature arguments.
        """
        vendor = self.args["vendor"]
        # Get asset type.
        if self.args["asset_type"] == "futures":
            data_type = f'{self.args["data_type"]}_futures'
        else:
            data_type = self.args["data_type"]
        # Get action tag.
        if self.args["action_tag"] == "downloaded_1sec":
            action_tag = "raw"
        elif self.args["action_tag"] == "resampled_1min":
            action_tag = "resampled_1min"
        else:
            action_tag = ""
        if action_tag:
            db_name = f"{vendor}_{data_type}_{action_tag}"
        else:
            # E.g. `ccxt_ohlcv_futures`
            db_name = f"{vendor}_{data_type}"
        return db_name

    def _build_s3_pq_file_path(self) -> str:
        """
        Get the path to the sample parquet file.

        TODO(Toma): this is a temporary solution, clean it up after data restructuring.
        """
        # Use the hardcoded base URL.
        s3_path_base = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq"
        # Get the full data type, e.g. `bid_ask` or `bid_ask-futures.`
        if self.args["asset_type"] == "futures":
            data_type = f'{self.args["data_type"]}-futures'
        else:
            data_type = self.args["data_type"]
        if self.args["vendor"] == "crypto_chassis":
            vendor = f'{self.args["vendor"]}.{self.args["action_tag"]}'
        else:
            vendor = self.args["vendor"]
        s3_dir_path = (
            f'{s3_path_base}/{data_type}/{vendor}/{self.args["exchange_id"]}'
        )
        return s3_dir_path
