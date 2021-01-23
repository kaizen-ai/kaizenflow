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
import vendors2.kibot.metadata.load.kibot_metadata as kmd

lfc = kmd.KibotHardcodedContractLifetimeComputer(365, 7)

lfc.compute_lifetime("CLJ17")

# %%
lfc = kmd.KibotTradingActivityContractLifetimeComputer()

lfc.compute_lifetime("CLJ17")

# %%
## 

# %%
symbols = ["ES", "CL"]
file = "./contracts.csv"

lfc = kmd.KibotHardcodedContractLifetimeComputer(365, 7)

kmd = kmd.ContractsLoader(symbols, file, lfc, refresh=True)
