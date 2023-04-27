"""
DAG to download stock market data.
"""


from airflow.models import DAG
from airflow.decorators import task

import time
from models.list_of_tickers import SP500
from models.ticker import Ticker
from models.time_series import TimeInterval, DataType
from api.mongo_db import Mongo
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="update_sp500",
    description="Downloads and updates S&P 500 data",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task
    def update():
        """Downloads S&P500 data in one minute intervals for the past trading day"""
        counter = 0

        for symbol in SP500:
            ticker = Ticker(symbol, get_name=False)
            ticker.get_data(data_type=DataType.INTRADAY,
                            time_interval=TimeInterval.ONE)
            Mongo.save_data(ticker)
            counter += 1

            if counter >= 5:
                counter = 0
                time.sleep(61) # Wait one minute

    @task
    def calculate_features():
        """Calculates RSI and moving averages and updates 
        the DB after the databse is updated"""

        tickers = Mongo.download()

        for ticker in tickers:
            ticker.calculate_stats()
            Mongo.update_ticker_stats(ticker)

    update() >> calculate_features()