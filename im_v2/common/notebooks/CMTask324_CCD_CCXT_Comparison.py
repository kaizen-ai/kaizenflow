# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import pandas as pd

import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client.clients as ivcdclcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client.clients as ivcdclcl
import im.cryptodatadownload.data.load.loader as icdalolo

import logging 

import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load the data

# %% [markdown]
# The code below can be used to load all the existing data from two vendors CDD and CCXT. Current version is specified to Binance only, however, even for one exchange there's too many data to operate, that's why I am skipping this part and allow downloads only for the universes.

# %% [markdown]
# ## CCXT

# %%
ccxt_client = imvcdclcl.CcxtFileSystemClient(data_type="ohlcv", root_dir="s3://alphamatic-data/data", aws_profile="am")
ccxt_universe = imvccunun.get_vendor_universe()
ccxt_binance_universe = [element for element in ccxt_universe if element.startswith("binance")]
#multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(class_ = ccxt_client, mode="concat")
#ccxt_data = multiple_symbols_client.read_data(ccxt_binance_universe) 

# %%
# need to sort by stamp
#ccxt_data = ccxt_data.sort_index()

# %%
#display(ccxt_data.head(3))
#display(ccxt_data.shape)

# %% [markdown]
# ## CDD

# %% run_control={"marked": false}
universe_cdd = imvccunun.get_vendor_universe(version="v01", vendor="CDD")
cdd_binance_universe_initial = [element for element in universe_cdd if element.startswith("binance")]

# %%
cdd_binance_universe = cdd_binance_universe_initial.copy()

# SCU_USDT has incorrect columns, so can not be downloaded
cdd_binance_universe.remove("binance::SCU_USDT")

# %%
#cdd_data=[]
#cdd_loader = icdalolo.CddLoader(root_dir="s3://alphamatic-data/data", aws_profile="am")

#for i in list(range(len(cdd_binance_universe))):
#    cdd_data.append(cdd_loader.read_data_from_filesystem(exchange_id="binance", 
#                                                            currency_pair=ivcdclcl.parse_full_symbol(cdd_binance_universe[i])[1], 
#                                                            data_type="ohlcv"))
#cdd_data = pd.concat(cdd_data)

# %%
#display(cdd_data.head(3))
#display(cdd_data.shape)

# %% [markdown]
# # Compare universes

# %%
print("Number of currency pairs for Binance in CCXT:", len(ccxt_universe))
print("aNumber of currency pairs for Binance in CDD:", len(universe_cdd))

# %%
# Intersection of currency pairs between two vendors
currency_pair_intersection = set(ccxt_universe).intersection(universe_cdd)
print("Number of similar currency pairs:", len(currency_pair_intersection))
display(currency_pair_intersection)

# %%
# Currency pairs that are included in CCXT but not in CDD
ccxt_and_not_cdd = set(ccxt_universe).difference(universe_cdd)
print("Number of unique CCXT currency pairs:", len(ccxt_and_not_cdd))
display(ccxt_and_not_cdd)

# %%
# Currency pairs that are included in CDD but not in CCXT
cdd_and_not_ccxt = set(universe_cdd).difference(ccxt_universe)
print("Number of unique CDD currency pairs:", len(cdd_and_not_ccxt))
display(cdd_and_not_ccxt)

# %% [markdown]
# # Compare returns

# %% [markdown]
# ## Load the data

# %% [markdown]
# We can compare only the intersection of currency pairs for two vendors. In this case it's specified for Binance exchange only.

# %%
currency_pair_intersection_binance = set(ccxt_binance_universe).intersection(cdd_binance_universe_initial)

# %%
cdd_data=[]
cdd_loader = icdalolo.CddLoader(root_dir="s3://alphamatic-data/data", aws_profile="am")

for i in list(range(len(currency_pair_intersection_binance))):
    cdd_data.append(cdd_loader.read_data_from_filesystem(exchange_id="binance", 
                                                            currency_pair=ivcdclcl.parse_full_symbol(list(currency_pair_intersection_binance)[i])[1], 
                                                            data_type="ohlcv"))
cdd_data = pd.concat(cdd_data)

# %%
display(cdd_data.head(3))
display(cdd_data.shape)

# %%
ccxt_client = imvcdclcl.CcxtFileSystemClient(data_type="ohlcv", root_dir="s3://alphamatic-data/data", aws_profile="am")
multiple_symbols_client = ivcdclcl.MultipleSymbolsClient(class_ = ccxt_client, mode="concat")
ccxt_data = multiple_symbols_client.read_data(currency_pair_intersection_binance) 

# %%
ccxt_data = ccxt_data.sort_index()

# %%
display(ccxt_data.head(3))
display(ccxt_data.shape)

# %% [markdown]
# ## Calculate returns and correlation

# %%
# CDD names cleaning
cdd_data["currency_pair"].replace({"BNB/USDT": "BNB_USDT", 
                                   "ETH/USDT": "ETH_USDT",
                                   "EOS/USDT": "EOS_USDT",
                                   "BTC/USDT": "BTC_USDT",
                                   "SOL/USDT": "SOL_USDT",
                                   "ADA/USDT": "ADA_USDT",
                                   "LINK/USDT": "LINK_USDT"}, inplace=True)


# %%
def calculate_correlations_of_returns(df_ccxt, df_cdd, resampling_freq):
    # CDD part
    df_cdd.reset_index(inplace=True)
    df_cdd = df_cdd.rename(columns={"index":"stamp"})
    resampler = df_cdd.groupby(["currency_pair", pd.Grouper(key="stamp",freq=resampling_freq)])
    close = resampler.close.last()
    grouper = close.groupby("currency_pair")
    rets_cdd = grouper.pct_change()
    # CCXT part
    df_ccxt.reset_index(inplace=True)
    df_ccxt = df_ccxt.rename(columns={"index":"stamp"})
    resampler = df_ccxt.groupby(["currency_pair", pd.Grouper(key="stamp",freq=resampling_freq)])
    close = resampler.close.last()
    grouper = close.groupby("currency_pair")
    rets_ccxt = grouper.pct_change()
    # Combine and calculate correlations
    rets_combined = pd.concat([rets_ccxt, rets_cdd],axis=1)
    rets_combined.columns=["ccxt_returns","cdd_returns"]
    corr_matrix = rets_combined.groupby(level=0).corr()
    return corr_matrix


# %%
returns_corr_5min = calculate_correlations_of_returns(ccxt_data, cdd_data,"5min")
returns_corr_5min

# %%
returns_corr_1day = calculate_correlations_of_returns(ccxt_data, cdd_data,"1D")
returns_corr_1day


# %% [markdown]
# # Comparison of Close prices

# %%
def calculate_correlations_of_close_prices(df_ccxt, df_cdd, resampling_freq):
    # CDD part
    cdd_data = df_cdd.rename(columns={"index":"stamp"})
    resampler = cdd_data.groupby(["currency_pair", pd.Grouper(key="stamp",freq=resampling_freq)])
    close_cdd = resampler.close.last()
    # CCXT part
    ccxt_data = df_ccxt.rename(columns={"index":"stamp"})
    resampler = ccxt_data.groupby(["currency_pair", pd.Grouper(key="stamp",freq=resampling_freq)])
    close_ccxt = resampler.close.last()
    # Correlation matrix calculations
    close_df = pd.concat([close_cdd,close_ccxt],axis=1)
    close_df.columns = ["cdd_close","ccxt_close"]
    corr_matrix = close_df.groupby(level=0).corr()
    display(corr_matrix)
    # DataFrame with description statistics
    descr_cdd = close_df.groupby(level=0)["cdd_close"].describe()
    descr_cdd=descr_cdd.add_suffix('_cdd')
    descr_ccxt = close_df.groupby(level=0)["ccxt_close"].describe()
    descr_ccxt=descr_ccxt.add_suffix('_ccxt')
    descr_df = pd.concat([descr_cdd, descr_ccxt],axis=1)
    descr_df.sort_index(ascending=False, axis=1,inplace=True)
    return descr_df


# %%
close_prices_stats = calculate_correlations_of_close_prices(ccxt_data, cdd_data,"1D")
close_prices_stats

# %% [markdown]
# # Statistical properties of a unique coin

# %%
# Clearing CDD currency pairs that are incorrect

# Binance
universe_cdd.remove("binance::SCU_USDT")

# Bitfinex
universe_cdd.remove("bitfinex::BTC_GBR") #doesn't exist
universe_cdd.remove("bitfinex::DASH_BTC") # NaT in stamps
universe_cdd.remove("bitfinex::DASH_USD") # NaT in stamps
universe_cdd.remove("bitfinex::EOS_GBR") #doesn't exist
universe_cdd.remove("bitfinex::ETH_GBR") #doesn't exist
universe_cdd.remove("bitfinex::NEO_GBR") #doesn't exist
universe_cdd.remove("bitfinex::QTUM_USD") # NaT in stamps
universe_cdd.remove("bitfinex::TRX_GBR") #doesn't exist
universe_cdd.remove("bitfinex::XLM_GBR") #doesn't exist

# ftx has some critical mistakes in the downloading process, so cannot continue analysis with them
cdd_ftx_universe = [element for element in universe_cdd if element.startswith("ftx")]
for elem in cdd_ftx_universe:
    universe_cdd.remove(elem) 


# %%
def calculate_statistics_for_stamps_cdd(coin_list):
    result=[]
    cdd_loader = icdalolo.CddLoader(root_dir="s3://alphamatic-data/data", aws_profile="am")
    for i in list(range(len(coin_list))):
        coin=cdd_loader.read_data_from_filesystem(exchange_id=ivcdclcl.parse_full_symbol(list(coin_list)[i])[0], 
                                                  currency_pair=ivcdclcl.parse_full_symbol(list(coin_list)[i])[1], 
                                                  data_type="ohlcv") 
        coin.reset_index(inplace=True)
        coin = coin.rename(columns={"index":"stamp"})
        stamp_stats=pd.DataFrame()
        stamp_steps = pd.Series(coin["stamp"].diff().value_counts().index)
        max_date=pd.Series(coin["stamp"].describe(datetime_is_numeric=True).loc["max"])
        min_date=pd.Series(coin["stamp"].describe(datetime_is_numeric=True).loc["min"])
        data_points = pd.Series(coin["stamp"].describe(datetime_is_numeric=True).loc["count"])
        stamp_stats["exchange_id"]=[ivcdclcl.parse_full_symbol(list(coin_list)[i])[0]]
        stamp_stats["data_points_counts"]=data_points
        stamp_stats["NaNs_in_Close"]=len(coin[coin["close"].isna()])
        stamp_stats["step_in_stamp"] = stamp_steps
        stamp_stats["start_date"]=min_date
        stamp_stats["end_date"]=max_date
        stamp_stats.index = [ivcdclcl.parse_full_symbol(list(coin_list)[i])[1]]
        result.append(stamp_stats)
    result = pd.concat(result)
    return result


# %%
stats_for_stamps = calculate_statistics_for_stamps_cdd(universe_cdd)

# %% [markdown]
# Currently there are  descriptive statistics:
# - __index__ - currency pair
# - __exchange_id__ - exchange_id
# - __data_points_counts__ - number of timestamps for each coin
# - __NaNs_in_Close__ - number of NaNs in "close"
# - __step_in_stamp__ - value counts of steps between timestamps
# - __start_date__
# - __end_date__
#
# What else can be added here?

# %%
stats_for_stamps

# %% [markdown]
# Each coin in CDD has a stamp step of 1 minute.

# %% [markdown]
# One can see that there are problems with __kucoin__ exchange: the timestamps are obviously wrong and wit too short time period.

# %%
typical_start_date = pd.DataFrame(stats_for_stamps[stats_for_stamps["exchange_id"]=="kucoin"]["start_date"].value_counts()).reset_index()["index"].dt.strftime('%d-%m-%Y').unique()
typical_end_date = pd.DataFrame(stats_for_stamps[stats_for_stamps["exchange_id"]=="kucoin"]["end_date"].value_counts()).reset_index()["index"].dt.strftime('%d-%m-%Y').unique()
print("Typical Start Date for Kucoin:", typical_start_date)
print("Typical End Date for Kucoin:", typical_end_date)
