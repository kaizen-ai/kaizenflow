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
# ## Imports

# %%
import os
import argparse
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah

# %% [markdown]
# ## Config
#
# To assist debugging you can override any of the config parameters after its loaded and rerun reconciler

# %%
# Load relevant environment variables.
config = filter(lambda x: x[0].startswith("DATA_RECONCILE_"), os.environ.items())
# Transform parameter names to the naming conventions used by the reconciler.
config = list(map(lambda x: x[0].lstrip("DATA_RECONCILE_").lower(), config))
# Dict can be passed as a namespace argument.
config = { it[0] : it[1] for it in config }

# %% [markdown]
# ## Initialize Reconciler

# %%
# The class was originally intended to be used via a cmdline script
args = argparse.Namespace(**config)
reconciler = imvcdecrah.RealTimeHistoricalReconciler(args)

# %%
