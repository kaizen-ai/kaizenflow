import airflow
from airflow.operators.bash import BashOperator
import helpers.datetime_ as hdateti
import datetime


# Pass default parameters for the DAG.
default_args = {"retries": 3,
                "retry_delay": datetime.timedelta(minutes=1),
                "email": ["d.tikhomirov@crypto-kaizen.com"],
                "email_on_failure": True,
                "owner": "test"}

dag = airflow.DAG(
    "realtime_ccxt",
    description="Realtime download of CCXT OHLCV data",

)

# #############################################################################

# Pass default parameters for the script.
script_args = {
    "dst_dir": "test/default_dir",
    "data_type": "ccxt_ohlcv",
    "universe": "v3",
    "api_keys": "API_keys.json",
    "table_name": "ccxt_ohlcv",
}

end_datetime = hdateti.get_timestamp("UTC")
start_datetime = hdateti.get_timestamp("UTC")

bash_command = f"python im_v2/ccxt/data/extract/download_realtime.py" \
               f"--start_datetime {start_datetime}" \
               f"--end_datetime {end_datetime}" \
               f"--dst_dir {script_args['dst_dir']}" \
               f"--data_type {script_args['data_type']}" \
               f"--table_name {script_args['table_name']}" \
               f"--api_keys {script_args['api_keys']}" \
               f"--universe {script_args['universe']}"

downloading_task = BashOperator(task_id="run_script",
                                bash_command=bash_command,
                                retries="3")
