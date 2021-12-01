import airflow
from airflow.operators.bash import BashOperator
import helpers.datetime_ as hdateti

# Pass default parameters.
args = {
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
               f"--dst_dir {args['dst_dir']}" \
               f"--data_type {args['data_type']}" \
               f"--table_name {args['table_name']}" \
               f"--api_keys {args['api_keys']}" \
               f"--universe {args['universe']}"


