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
# is via invoke target `dev_scripts.lib_tasks_reconcile.run_data_reconciliation_notebook`

# %% [markdown]
# ## Imports and logging

# %%
import os
import argparse
import logging
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
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
os.environ.items()

# %%
# Load relevant environment variables.
config = filter(lambda x: x[0].startswith("DATA_RECONCILE_"), os.environ.items())
# Transform parameter names to the naming conventions used by the reconciler.
config = list(map(lambda x: (x[0].replace("DATA_RECONCILE_", "").lower(), x[1]), config))
# Dict can be passed as a namespace argument.
config = { it[0] : it[1] for it in config }
config

# %%
# Bid_ask_accuracy needs special treatment
#  since int is expected in the reconciler.
config['bid_ask_accuracy'] = None if config['bid_ask_accuracy'] == 'None' else int(config['bid_ask_accuracy'])
# Transform resample_mode to parameter supported
#  by the reconciler
config["resample_1sec"] = config["resample_mode"] == "resample_1sec"
config["resample_1min"] = config["resample_mode"] == "resample_1min"
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
reconciler.run()
