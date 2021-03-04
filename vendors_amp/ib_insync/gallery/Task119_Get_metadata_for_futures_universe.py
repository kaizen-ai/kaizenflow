# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import ib_insync
print(ib_insync.__all__)

import helpers.dbg as dbg
import helpers.printing as pri
import core.explore as exp
import vendors_lemonade.ib_insync.utils as ibutils

# %%
ib = ibutils.ib_connect(client_id=33, is_notebook=True)

# %%
# Look for ES.

#symbol = "ES"
symbol = "NG"
#symbol = "CL"
asset = ib_insync.Future(symbol, includeExpired=True)
#ibutils.get_contract_details(ib, asset)

cds = ib.reqContractDetails(asset)

contracts = [cd.contract for cd in cds]

ib_insync.util.df(contracts)
