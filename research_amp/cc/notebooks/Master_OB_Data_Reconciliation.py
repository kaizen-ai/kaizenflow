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
# TODO(Max): convert to master notebook.
# TODO(Max): the notebook is runnable only from branch: `CMTask2703_Perform_manual_reconciliation_of_OB_data`.

# %% [markdown]
# - CCXT data = CCXT real-time DB bid-ask data collection for futures
# - CC data = CryptoChassis historical Parquet bid-ask futures data

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_example_config() -> cconfig.Config:
    """
    Config for comparison of 1sec CryptoChassis and 1sec CCXT bid/ask data
    """
    # TODO(Danya): parameters: 1sec/1min, bid/ask//ohclv, vendors
    config = cconfig.Config()
    param_dict = {
        "data": {
            # Parameters for client initialization.
            "cc_im_client": {
                "universe_version": None,
                "resample_1min": False,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"),
                    "reorg",
                    "daily_staged.airflow.pq",
                ),
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                "data_snapshot": "",
                "aws_profile": "ck",
            },
            "ccxt_im_client": {
                "resample_1min": False,
                "db_connection": hsql.get_connection(
                    *hsql.get_connection_info_from_env_file(
                        imvimlita.get_db_env_path("dev")
                    )
                ),
                "table_name": "ccxt_bid_ask_futures_raw",
            },
            # Parameters for data query.
            "read_data": {
                # Get start/end ts as inputs to script.
                "start_ts": pd.Timestamp("2022-11-16 00:00:00+00:00"),
                "end_ts": pd.Timestamp("2022-11-16 00:00:15+00:00"),
                "columns": None,
                "filter_data_mode": "assert",
            },
        },
        "column_names": {
            "bid_ask_cols": [
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
                "full_symbol",
            ],
        },
        "order_level": 1,
    }
    config = cconfig.Config.from_dict(param_dict)
    return config


config = get_example_config()
print(config)

# %% [markdown]
# # Clients

# %%
# CCXT client.
ccxt_im_client = icdcl.CcxtSqlRealTimeImClient(**config["data"]["ccxt_im_client"])
# CC client.
cc_parquet_client = iccdc.CryptoChassisHistoricalPqByTileClient(
    **config["data"]["cc_im_client"]
)

# %% [markdown]
# # Universe

# %%
# DB universe
ccxt_universe = ccxt_im_client.get_universe()
# CC universe.
cc_universe = cc_parquet_client.get_universe()
# Intersection of universes that will be used for analysis.
universe = list(set(ccxt_universe) & set(cc_universe))

# %%
compare_universe = hprint.set_diff_to_str(
    cc_universe, ccxt_universe, add_space=True
)
print(compare_universe)

# %% [markdown]
# # Load data

# %% run_control={"marked": true}
ccxt_df = ccxt_im_client.read_data(universe, **config["data"]["read_data"])

# %%
ccxt_df

# %%
cc_df = cc_parquet_client.read_data(universe, **config["data"]["read_data"])

# %%
