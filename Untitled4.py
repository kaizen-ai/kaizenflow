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
import logging

import market_data as mdata
import im_v2.ccxt.data.client as icdcl
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
universe_version = "v7"
resample_1min = False
dataset = "ohlcv"
contract_type = "futures"
data_snapshot = ""
im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
    universe_version,
    resample_1min,
    dataset,
    contract_type,
    data_snapshot,
)
# 
full_symbols = im_client.get_universe()
asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
columns = None
columns_remap = None

market_data = mdata.get_HistoricalImClientMarketData_example1(
    im_client,
    asset_ids,
    columns,
    columns_remap,
)

# %%
import helpers.hpandas as hpandas
import pandas as pd

# %%
df1 = pd.DataFrame(data={1: 1.1, 2: 2.2}, index=["a", "b"])
df2 = pd.DataFrame(data={3: 1.1, 4: [2.2, 3.32, 4.33], 5: 0}, index=["b", "a", "c"])
df1

# %%
df2

# %%
df2.index = sorted(df2.index)
df2

# %%
df2.sort_index()

# %%
hpandas.dassert_indices_equal(df1, df2)

# %%
hdbg.dassert_eq(df1.shape, df2.shape)

# %%
a = {
    2: 34,
    4: 49,
    1: 345,
    0: 348,
    3: 54,
}

# %%
a = sorted(a.items())

# %%
dict(a)
