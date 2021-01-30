# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import logging

import pandas as pd
import sklearn as sk

import core.dataframe_modeler as dfmod
import core.signal_processing as sigp
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt
import vendors2.kibot.data.load.futures_forward_contracts as vkdlfu
import vendors2.kibot.data.load.s3_data_loader as vkdls3
import vendors2.kibot.metadata.load.kibot_metadata as vkmlki

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", env.get_system_signature()[0])

prnt.config_notebook()

# %% [markdown]
# ## Map contracts to start and end dates

# %%
lfc_hc = vkmlki.KibotHardcodedContractLifetimeComputer(365, 7)

lfc_hc.compute_lifetime("CLJ17")

# %%
lfc_ta = vkmlki.KibotTradingActivityContractLifetimeComputer()

lfc_ta.compute_lifetime("CLJ17")

# %%
symbols = ["ES", "CL"]
file = "../contracts.csv"


fcl = vkmlki.FuturesContractLifetimes(file, lfc_hc)

# %%
fcl.save(["CL"])

# %%
cl_data = fcl.load(["CL"])

# %%
cl_data["CL"].head()

# %% [markdown]
# ## Create continuous contracts

# %%
fcem = vkmlki.FuturesContractExpiryMapper(cl_data)

# %%
fcem.get_nth_contract("CL", "2010-01-01", 1)

# %%
srs = fcem.get_nth_contracts("CL", "2010-01-10", "2010-01-20", freq="B", n=1)

# %%
srs

# %%
kdl = vkdls3.S3KibotDataLoader()

# %%
ffc_obj = vkdlfu.FuturesForwardContracts(kdl)

# %%
ffc_obj._replace_contracts_with_data(srs)

# %% [markdown]
# ## Combine front and back contracts

# %%
cl_df = fcem.get_contracts(["CL1", "CL2", "CL3", "CL4"],
                           "2010-01-01",
                           "2015-12-31",
                           freq="B")

# %%
cl_df.head()

# %%
price_df = ffc_obj.replace_contracts_with_data(cl_df, "close")

# %%
price_df.plot()

# %%
dfm = dfmod.DataFrameModeler(df=price_df, oos_start="2013-01-01") \
          .compute_ret_0(method="predict") \
          .apply_column_transformer(
              transformer_func=sigp.compute_rolling_zscore,
              transformer_kwargs={
                  "tau": 10,
                  "min_periods": 20,
              },
              col_mode="replace_all",
              method="predict",
          )

# %%
dfm.plot_time_series()

# %%
dfm.plot_pca_components()

# %%
dfm.plot_explained_variance()

# %%
res = dfm.apply_residualizer(
    model_func=sk.decomposition.PCA,
    x_vars=["CL1_ret_0", "CL2_ret_0", "CL3_ret_0", "CL4_ret_0"],
    model_kwargs={
        "n_components": 1
    },
    method="predict"
)

# %%
res.df.plot()

# %%
