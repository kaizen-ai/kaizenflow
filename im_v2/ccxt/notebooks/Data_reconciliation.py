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

# %% [markdown]
# # Data reconciliation
#
# This notebook is used to perform reconciliation on data obtained in realtime with batch data downloaded at once, i.e. once a day.
# As displayed below, the notebook assumes environment variables for the reconciliation parameters. The intended use
# is via invoke target `dev_scripts.lib_tasks_reconcile.reconcile_data_run_notebook`

# %% [markdown]
# ## Imports and logging

# %%
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hio as hio
import core.config as cconfig
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah

# %% [markdown]
# ### Logging

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## Reconciliation parameters
#
# To assist debugging you can override any of the parameters after its loaded and rerun reconciler

# %%
env_var_name = "CK_DATA_RECONCILIATION_CONFIG"
config = cconfig.Config.from_env_var(env_var_name)

# %%
# Transform resample_mode to parameter supported
#  by the reconciler
config["resample_1sec"] = config["resample_mode"] == "resample_1sec"
config["resample_1min"] = config["resample_mode"] == "resample_1min"

# %%
config = config.to_dict()
# bid_ask_accuracy needs to be cast to int if its defined
config["bid_ask_accuracy"] = int(config["bid_ask_accuracy"]) if config["bid_ask_accuracy"] else None
config

# %% [markdown]
# ## Initialize Reconciler

# %%
# The class was originally intended to be used via a cmdline script
args = argparse.Namespace(**config)
reconciler = imvcdecrah.RealTimeHistoricalReconciler(args)

# %%
# CCXT Realtime data
reconciler.ccxt_rt.head()

# %%
# Historical data
reconciler.daily_data.head()

# %% [markdown]
# ## Run reconciliation

# %%
try:
    reconciler.run()
except Exception as e:
    # Pass information about success or failure of the reconciliation
    #  back to the task that invoked it.
    data_reconciliation_outcome = str(e)
    raise e
# If no exception was raised mark the reconciliation as successful.
data_reconciliation_outcome = "SUCCESS"

# %%
# This can be read by the invoke task to find out if reconciliation was successful.
hio.to_file("/app/ck_data_reconciliation_outcome.txt", data_reconciliation_outcome)
