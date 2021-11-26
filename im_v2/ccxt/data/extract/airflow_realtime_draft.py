import airflow
import im_v2.ccxt.data.extract.download_realtime as imvcdedore
from airflow.operators.python import PythonOperator
import helpers.sql as hsql

# TODO(Danya): A placeholder until the interface is cleared up.
start_date = input()
end_date = input()

dag = airflow.DAG(
    dag_id="download_ccxt_ohlcv",
    # TODO(Danya): Start and end date provided at each launch.
    #  Wrap into a daily job that provides start and end?
    start_date=start_date,
    end_date=end_date,
    # Run each minute.
    schedule_interval="*/1 * * * *",
    # Do not backfill.
    catchup=False
    )


def _load_universe():
    pass


def _extract_data():
    pass


def _save_to_db():
    pass


def _save_to_disk():
    pass


def remove_db_duplicates():
    pass


load_universe >> extract_data >> save_to_db >> remove_db_duplicates
load_universe >> extract_data >> save_to_disk