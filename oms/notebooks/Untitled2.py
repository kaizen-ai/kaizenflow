# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

# %%
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita

# %%
resample_1min = False
env_file = imvimlita.get_db_env_path("dev")
# Get login info.
connection_params = hsql.get_connection_info_from_env_file(env_file)
# Login.
db_connection = hsql.get_connection(*connection_params)
# Get the real-time `ImClient`.
# TODO(Grisha): this will print only the `futures` universe, allow also
# to print `spot` universe.
table_name = "ccxt_ohlcv_futures"
#
im_client = icdcl.CcxtSqlRealTimeImClient(
    resample_1min, db_connection, table_name
)

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
vendor = "CCXT"
mode = "trade"
version = "v7.1"
as_full_symbol = True
full_symbols = ivcu.get_vendor_universe(
    vendor,
    mode,
    version=version,
    as_full_symbol=as_full_symbol,
)
full_symbols

# %%
start_ts = pd.Timestamp("2022-10-25 00:00:00-04:00")
end_ts = pd.Timestamp("2022-10-25 23:59:00-04:00")
columns = None
filter_data_mode = "assert"
df = im_client.read_data(
    full_symbols, start_ts, end_ts, columns, filter_data_mode
)
df["delta"] = (df["knowledge_timestamp"] - df.index).dt.total_seconds()
df

# %%
df.groupby(by=["full_symbol"]).max()["delta"].sort_values(ascending=False).plot(
    kind="bar"
)

# %%
print("min", df["delta"].min().round(2))
print("mean", df["delta"].mean().round(2))
print("std", df["delta"].std().round(2))
print("max", df["delta"].max().round(2))

# %%
df["delta"].plot(kind="hist", bins=50, xlim=(0, 120))

# %%
path = "/shared_data/prod_reconciliation/20221013/tca/json/fills_20221012-000000_20221013-000000_binance.preprod.trading.3.json"
file = hio.from_json(path)

# %%
df = pd.DataFrame(file)
df
