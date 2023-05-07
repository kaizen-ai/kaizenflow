"""
DAG to download OHLCV data from CoinGecko
"""
import airflow 
import os
import logging
from airflow.models import Variable , TaskInstance
from airflow import DAG, settings

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import datetime
import pendulum 

# from airflow.operators.bash import BashOperator

_DAG_ID = "download_periodic_1min_postgres_coingecko"
_DAG_DESCRIPTION = (
    "Download Coingecko data every hour and save to Postgres"
)
# Specify when to execute the DAG.
_SCHEDULE = '0 * * * *'

# Pass default parameters for the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2023, 4, 15, 0, 0, 0),
)

# create dynamic variables for data_invterval_start/end
def bridge(data_interval_start):

    start_str = str(data_interval_start)
    from_timestamp = pendulum.parse(start_str).int_timestamp
    fromValue = Variable.get("from_timestamp1")

    logging.info("Current from_timestamp1 value is" + str(fromValue))

    logging.info("Setting Airflow Variable from_timestamp1 to" + str(fromValue))
    os.system('airflow variables --set from_timestamp1' + str(fromValue))

def bridge2(data_interval_end):

    data_str = str(data_interval_end)
    to_timestamp = pendulum.parse(data_str).int_timestamp
    toValue = Variable.get("to_timestamp1")

    logging.info("Current from_timestamp1 value is" + str(toValue))

    logging.info("Setting Airflow Variable from_timestamp1 to" + str(toValue))
    os.system('airflow variables --set from_timestamp1' + str(toValue))



from_timestamp1 = Variable.get("from_timestamp1")
logging.info("The current from_timestamp1 value is" + str(from_timestamp1))


to_timestamp1 = Variable.get("to_timestamp1")
logging.info("The current to_timestamp1 value is" + str(to_timestamp1))

bridge_task = PythonOperator(task_id='bridge', dag=dag, provide_context=True, python_callable=bridge, op_args=[])
bridge_task2 = PythonOperator(task_id='bridge2', dag=dag, provide_context=True, python_callable=bridge2, op_args=[])

bash_command = [
    # Sleep 5 seconds to ensure the bar is finished.
    "sleep 5",
    "&&",
    "/cmamp/sorrentum_sandbox/examples/ml_projects/Issue29_Team10_Implement_sandbox_for_coingecko/dowonload_to_db.py",
    "--id bitcoin",
    "--from_timestamp ${from_timestamp}",
    "--to_timestamp ${to_timestamp}",
    "--target_table 'coingecko_historic'",
    "-v DEBUG"
]

downloading_task = BashOperator(
    task_id="download.periodic_1min.postgres.ohlcv.coingecko",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    env = {"from_timestamp":"{{ var.value.get('from_timestamp1') }}", "to_timestamp":"{{ var.value.get('to_timestamp1') }}"},
    dag=dag,
)

bridge_task.set_downstream(bridge_task2)
bridge_task2.set_downstream(downloading_task)

