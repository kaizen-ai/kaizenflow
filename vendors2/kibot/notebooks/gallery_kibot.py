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

# %%

import vendors2.kibot.data.load.futures_forward_contracts as vkdlfu
import vendors2.kibot.data.load.s3_data_loader as vkdls3
import vendors2.kibot.metadata.load.kibot_metadata as vkmlki

# %%
lfc_hc = vkmlki.KibotHardcodedContractLifetimeComputer(365, 7)

lfc_hc.compute_lifetime("CLJ17")

# %%
lfc_ta = vkmlki.KibotTradingActivityContractLifetimeComputer()

lfc_ta.compute_lifetime("CLJ17")

# %%
##

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

# %%

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

# %%
