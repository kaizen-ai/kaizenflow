"""
Manage (e.g., create, destroy, query) IM Postgres DB.

Import as:

import im_v2.common.db.db_utils as imvcddbut
"""
import abc
import argparse
import logging
import os
from datetime import timedelta
from typing import Optional

import pandas as pd
import psycopg2 as psycop

import helpers.hdatetime as hdateti
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsql as hsql
import helpers.hsql_test as hsqltest
import im.ib.sql_writer as imibsqwri
import im.kibot.sql_writer as imkisqwri
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.im_lib_tasks as imvimlita
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


# Set of columns which are unique row-wise across db tables for
#  corresponding data type
BASE_UNIQUE_COLUMNS = ["timestamp", "exchange_id", "currency_pair"]
BID_ASK_UNIQUE_COLUMNS = []
OHLCV_UNIQUE_COLUMNS = BASE_UNIQUE_COLUMNS + [
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def add_db_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for db table and stage.
    """
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument(
        "--db_table",
        action="store",
        required=True,
        type=str,
        help="DB table to use",
    )
    return parser


def get_common_create_table_query() -> str:
    """
    Get SQL query that is used to create tables for common usage.
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS Exchange (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        name text UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Symbol (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        code text UNIQUE,
        description text,
        asset_class AssetClass,
        start_date date DEFAULT CURRENT_DATE,
        symbol_base text
    );

    CREATE TABLE IF NOT EXISTS TRADE_SYMBOL (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        exchange_id integer REFERENCES Exchange,
        symbol_id integer REFERENCES Symbol,
        UNIQUE (exchange_id, symbol_id)
    );
    """
    return sql_query


def get_data_types_query() -> str:
    """
    Define custom data types inside a database.
    """
    # Define data types.
    query = """
    CREATE TYPE AssetClass AS ENUM ('futures', 'etfs', 'forex', 'stocks', 'sp_500');
    CREATE TYPE Frequency AS ENUM ('minute', 'daily', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    return query


def create_all_tables(db_connection: hsql.DbConnection) -> None:
    """
    Create all the tables inside an IM database.

    :param db_connection: a database connection
    """
    queries = [
        get_data_types_query(),
        get_common_create_table_query(),
        imibsqwri.get_create_table_query(),
        imkisqwri.get_create_table_query(),
        imvccdbut.get_ccxt_ohlcv_create_table_query(),
        imvccdbut.get_ccxt_ohlcv_futures_create_table_query(),
        imvccdbut.get_ccxt_create_bid_ask_raw_table_query(),
        imvccdbut.get_ccxt_create_bid_ask_futures_raw_table_query(),
        imvccdbut.get_ccxt_create_bid_ask_resampled_1min_table_query(),
        imvccdbut.get_ccxt_create_bid_ask_futures_resampled_1min_table_query(),
        imvccdbut.get_exchange_name_create_table_query(),
        imvccdbut.get_currency_pair_create_table_query(),
    ]
    # Create tables.
    for query in queries:
        _LOG.debug("Executing query %s", query)
        try:
            cursor = db_connection.cursor()
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("Duplicate table created, skipping.")


def create_im_database(
    db_connection: hsql.DbConnection,
    new_db: str,
    *,
    overwrite: Optional[bool] = None,
) -> None:
    """
    Create database and SQL schema inside it.

    :param db_connection: a database connection
    :param new_db: name of database to connect to, e.g. `im_db_local`
    :param overwrite: overwrite existing database
    """
    _LOG.debug("connection=%s", db_connection)
    # Create a DB.
    hsql.create_database(db_connection, dbname=new_db, overwrite=overwrite)
    conn_details = hsql.db_connection_to_tuple(db_connection)
    new_db_connection = hsql.get_connection(
        host=conn_details.host,
        dbname=new_db,
        port=conn_details.port,
        user=conn_details.user,
        password=conn_details.password,
    )
    # Create table.
    create_all_tables(new_db_connection)
    new_db_connection.close()


def delete_duplicate_rows_from_ohlcv_table(db_stage: str, db_table: str) -> None:
    """
    Delete duplicate data rows from a db table which uses a standard OHLCV
    table format.

    In this context, a row is considered duplicate, when
    there exists another row which has identical timestamp, currency pair
    and exchange ID but different row ID and knowledge timestamp.

    :param db_stage: database stage to connect to
    :param db_table: database table to delete duplicates from
    """
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    db_connection = hsql.get_connection(*connection_params)
    delete_query = hsql.get_remove_duplicates_query(
        table_name=db_table,
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    num_before = hsql.get_num_rows(db_connection, db_table)
    hsql.execute_query(db_connection, delete_query)
    num_after = hsql.get_num_rows(db_connection, db_table)
    _LOG.warning(
        "Removed %s duplicate rows from %s table.",
        str(num_before - num_after),
        db_table,
    )


def fetch_last_minute_bid_ask_rt_db_data(
    db_connection: hsql.DbConnection, src_table: str, time_zone: str
) -> pd.Timestamp:
    """
    Fetch last full minute of bid/ask RT data.
    
    E.g. when the script is called at 9:05:05AM, The functions
    return data where timestamp is in interval [9:04:00, 9:05). 

    This is a convenience wrapper function to make the most likely use
    case easier to execute.
    """
    end_ts = hdateti.get_current_time(time_zone).floor("min")
    start_ts = end_ts - timedelta(minutes=1)
    return fetch_bid_ask_rt_db_data(db_connection, src_table, start_ts, end_ts)


def fetch_bid_ask_rt_db_data(
    db_connection: hsql.DbConnection,
    src_table: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.Timestamp:
    """
    Fetch bid/ask data for specified interval.

    Data interval is applied as: [start_ts, end_ts).

    :param db_connection: a database connection object
    :param src_table: name of the table to select from
    :param start_ts: start of the time interval to resample
    :param end_ts: end of the time interval to resample
    """
    # TODO(Juraj): perform bunch of assertions.
    start_ts_unix = hdateti.convert_timestamp_to_unix_epoch(start_ts)
    end_ts_unix = hdateti.convert_timestamp_to_unix_epoch(end_ts)
    select_query = f"""
                    SELECT * FROM {src_table} WHERE timestamp >= {start_ts_unix}
                    AND timestamp < {end_ts_unix};
                    """
    return hsql.execute_query_to_df(db_connection, select_query)


def fetch_data_by_age(
    timestamp: pd.Timestamp,
    db_connection: hsql.DbConnection,
    db_table: str,
    table_timestamp_column: str,
) -> pd.DataFrame:
    """
    Fetch data strictly older than a specified timestamp from a db table.

    Age is determined based on a specified column, i.e.
    when

    :param db_connection: a database connection object
    :param db_table: name of the table to select from
    :param table_timestamp_column: name of the column to apply the comparison on
    :param timestamp: timestamp to filter on
    :return DataFrame with data older than the specified `timestamp` based
     on `table_column` value.
    """
    ts_unix = hdateti.convert_timestamp_to_unix_epoch(timestamp)
    select_query = f"""
                    SELECT * FROM {db_table} WHERE {table_timestamp_column} < {ts_unix};
                    """
    return hsql.execute_query_to_df(db_connection, select_query)


def drop_db_data_by_age(
    timestamp: pd.Timestamp,
    db_connection: hsql.DbConnection,
    db_table: str,
    table_column: str,
) -> None:
    """
    Delete data strictly older than a specified timestamp from a db table.

    Age is determined based on a specified column.

    :param db_connection: a database connection object
    :param db_table: name of the table to delete from
    :param table_column: name of the column to apply the comparison on
    :param timestamp: timestamp to filter on
    """
    ts_unix = hdateti.convert_timestamp_to_unix_epoch(timestamp)
    delete_query = f"""
                    DELETE FROM {db_table} WHERE {table_column} < {ts_unix};
                    """
    hsql.execute_query(db_connection, delete_query)


# TODO(Juraj): replace all occurrences of code inserting to db with a call to
# this function.
# TODO(Juraj): probabl hsql is a better place for this?
def save_data_to_db(
    data: pd.DataFrame,
    data_type: str,
    db_connection: hsql.DbConnection,
    db_table: str,
    time_zone: str,
) -> None:
    """
    Save data into specified database table.

    INSERT query logic ensures exact duplicates are not saved into the database again.

    :param data: data to insert into database.
    :param db_connection: a database connection object
    :param db_table: name of the table to insert to.
    :param time_zone: time zone used to add correct knowledge_timestamp to the data
    """
    if data.empty:
        _LOG.warning("The DataFame is empty, nothing to insert.")
        return
    data = imvcdttrut.add_knowledge_timestamp_col(data, "UTC")
    if data_type == "ohlcv":
        unique_columns = OHLCV_UNIQUE_COLUMNS
    elif data_type == "bid_ask":
        unique_columns = BID_ASK_UNIQUE_COLUMNS
    else:
        raise ValueError(f"Invalid data_type='{data_type}'")
    hsql.execute_insert_on_conflict_do_nothing_query(
        connection=db_connection,
        obj=data,
        table_name=db_table,
        unique_columns=unique_columns,
    )


# #############################################################################
# TestImDbHelper
# #############################################################################


# TODO(gp): Move to db_test_utils.py


class TestImDbHelper(hsqltest.TestImOmsDbHelper, abc.ABC):
    """
    Configure the helper to build an IM test DB.
    """

    # TODO(gp): For some reason without having this function defined, the
    #  derived classes can't be instantiated because of get_id().
    @classmethod
    @abc.abstractmethod
    def get_id(cls) -> int:
        raise NotImplementedError

    @classmethod
    def _get_compose_file(cls) -> str:
        idx = cls.get_id()
        dir_name = hgit.get_amp_abs_path()
        docker_compose_path = os.path.join(
            dir_name, "im_v2/devops/compose/docker-compose.yml"
        )
        docker_compose_path_idx: str = hio.add_suffix_to_filename(
            docker_compose_path, idx
        )
        return docker_compose_path_idx

    @classmethod
    def _get_service_name(cls) -> str:
        idx = cls.get_id()
        return "im_postgres" + str(idx)

    # TODO(gp): Use file or path consistently.
    @classmethod
    def _get_db_env_path(cls) -> str:
        """
        See `_get_db_env_path()` in the parent class.
        """
        # Use the `local` stage for testing.
        idx = cls.get_id()
        env_file_path = imvimlita.get_db_env_path("local", idx=idx)
        return env_file_path  # type: ignore[no-any-return]

    @classmethod
    def _get_postgres_db(cls) -> str:
        return "im_postgres_db_local"