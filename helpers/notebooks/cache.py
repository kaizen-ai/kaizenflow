# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown] pycharm={"name": "#%% md\n"}
# # Test Cache in Jupyter Notebook

# %%
import helpers.dbg as dbg
import helpers.cache as hcac


# %% [markdown] pycharm={"name": "#%% md\n"}
# # Define computation function
#
# This function will be subjected to cache.

# %% pycharm={"name": "#%%\n"}
def computation_function(a, b):
    return a + b


# %% [markdown] pycharm={"name": "#%% md\n"}
# ## Memory cache

# %% pycharm={"name": "#%%\n"}
memory_cached_computation = hcac.Cached(computation_function, use_mem_cache=True, use_disk_cache=False)

dbg.dassert_eq(memory_cached_computation(1, 2), 3)
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(memory_cached_computation(1, 2), 3)
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "mem")

print("memory caching checks passed")

# %% [markdown]
# ## Disk cache

# %% pycharm={"name": "#%%\n"}
disk_cached_computation = hcac.Cached(computation_function, use_mem_cache=False, use_disk_cache=True)

disk_cached_computation.clear_disk_cache()

dbg.dassert_eq(disk_cached_computation(3, 2), 5)
dbg.dassert_eq(disk_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(disk_cached_computation(3, 2), 5)
dbg.dassert_eq(disk_cached_computation.get_last_cache_accessed(), "disk")

print("disk caching checks passed")

# %% [markdown]
# ## Full cache

# %% pycharm={"name": "#%%\n"}
fully_cached_computation = hcac.Cached(computation_function, use_mem_cache=True, use_disk_cache=True)

fully_cached_computation.clear_disk_cache()

dbg.dassert_eq(fully_cached_computation(3, 2), 5)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(fully_cached_computation(3, 2), 5)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")

fully_cached_computation.clear_memory_cache()

dbg.dassert_eq(fully_cached_computation(3, 2), 5)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "disk")

print("full caching checks passed")
