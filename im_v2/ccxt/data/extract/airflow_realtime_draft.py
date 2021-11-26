import airflow

import helpers.sql as hsql
import im_v2.ccxt.data.extract.download_realtime as imvcdedore
import im_v2.common.universe.universe as imvcounun

# TODO(Danya): A placeholder until the interface is cleared up.
start_date = input()
end_date = input()

# TODO(*): Not clear at what point should the connection be generated.
connection = hsql.get_connection_from_env_vars()

dag = airflow.DAG(
    dag_id="download_ccxt_ohlcv",
    # TODO(Danya): Start and end date provided at each launch.
    #  Wrap into a daily job that provides start and end?
    start_date=start_date,
    end_date=end_date,
    # Run each minute.
    schedule_interval="*/1 * * * *",
    # Do not backfill.
    catchup=False,
)


def _load_universe(universe: str, api_keys: str):
    """
    Load universe and exchanges.

    :param universe: name of universe, e.g. 'v3'
    :param api_keys: path to API keys JSON
    :return: instantiated exchanges
    """
    # TODO(Danya): This operation should not be conducted every minute,
    #  but at the launch of the Airflow job.
    universe = imvcounun.get_trade_universe(universe)
    exchange_ids = universe["CCXT"].keys()
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(
            imvcdedore._instantiate_exchange(
                exchange_id, universe["CCXT"], api_keys
            )
        )
    return exchanges


def _extract_data(exchanges, data_type: str) -> List[pd.DataFrame]:
    """
    Download 5-minute data for each exchange/currency.

    :param exchanges: instantiated exchanges
    # TODO(Danya): This should be provided from outside.
    :param data_type: "ohlcv" or "orderbook"
    :return: a list of downloaded OHLCV 5-minute data
    """
    pairs_iteration = []
    for exchange in exchanges:
        for pair in exchange.pairs:
            # Download latest data.
            pair_data = imvcdedore._download_data(data_type, exchange, pair)
            pairs_iteration.append(pair_data)
    return pairs_iteration


def _save_to_db(pairs_iteration) -> None:
    """
    Save everything into DB.
    :param pairs_iteration:
    """
    # TODO(Danya): Concatenate and load as a single df.
    for pair_data in pairs_iteration:
        hsql.execute_insert_query(
            connection=connection,
            obj=pair_data,
            # TODO(Danya): Table_name should be provided from outside.
            table_name=table_name,
        )


def _save_to_disk(pairs_iteration: List[pd.DataFrame], dst_dir) -> None:
    """
    Save downloaded data to disk.

    :param pairs_iteration: a list of downloaded DFs for exchange/currencies
    # TODO(Danya): This should be provided from outside.
    :param dst_dir: name of dir to save to
    :return:
    """
    # TODO(Danya): Concatenate and save as a single dataframe.
    for pair_data in pairs_iteration:
        imvcdedore._save_data_on_disk(
            data_type="ohlcv", dst_dir=dst_dir, pair_data=pair_data
        )


def remove_db_duplicates(connection: hsql.DbConnection):
    """
    Remove duplicate rows from the database.
    :param connection: connection to the database
    """
    # TODO(Danya): Include this into save_to_db task.
    dup_query = hsql.get_remove_duplicates_query(
        table_name="ccxt_ohlcv",
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    connection.cursor().execute(dup_query)


# TODO(Danya): Add operators once interfaces are done with.


# TODO(Danya): Simplify the chain (see DAGs in Airflow official docs).
load_universe >> extract_data >> save_to_db >> remove_db_duplicates
load_universe >> extract_data >> save_to_disk
