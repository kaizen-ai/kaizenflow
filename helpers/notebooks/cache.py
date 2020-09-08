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
import helpers.cache as hcac

# %%
import helpers.dbg as dbg

# %% [markdown] pycharm={"name": "#%% md\n"}
# # Define computation function
#
# This function will be subjected to cache.

# %%
def computation_function(a, b):
    # hello
    # assert 0
    out = a * b
    print("Multiplication: %s * %s = %s" % (a, b, out))
    return out


inputs = (1, 2)
exp_output = 2

computation_function(*inputs)

# %% [markdown] pycharm={"name": "#%% md\n"}
# ## Memory cache

# %% pycharm={"name": "#%%\n"}
memory_cached_computation = hcac.Cached(
    computation_function, use_mem_cache=True, use_disk_cache=False
)

memory_cached_computation.clear_cache()

dbg.dassert_eq(memory_cached_computation(*inputs), exp_output)
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(memory_cached_computation(*inputs), exp_output)
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "mem")

print("memory caching checks passed")


# %%
def computation_function(a, b):
    # hello
    # assert 0
    out = a * b
    print("Multiplication: %s * %s = %s" % (a, b, out))
    return out


inputs = (1, 2)
exp_output = 2

dbg.dassert_eq(memory_cached_computation(*inputs), exp_output)
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "mem")

# %% [markdown]
# ## Disk cache

# %% pycharm={"name": "#%%\n"}
disk_cached_computation = hcac.Cached(
    computation_function, use_mem_cache=False, use_disk_cache=True
)

disk_cached_computation.clear_cache("disk")

dbg.dassert_eq(disk_cached_computation(*inputs), exp_output)
dbg.dassert_eq(disk_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(disk_cached_computation(*inputs), exp_output)
dbg.dassert_eq(disk_cached_computation.get_last_cache_accessed(), "disk")

print("disk caching checks passed")

# %% [markdown]
# ## Full cache

# %% pycharm={"name": "#%%\n"}
fully_cached_computation = hcac.Cached(
    computation_function, use_mem_cache=True, use_disk_cache=True
)

fully_cached_computation.clear_cache()

dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "no_cache")

dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")

dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")

print("Clear mem cache")
fully_cached_computation.clear_cache("mem")

dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "disk")

dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")

print("full caching checks passed")

# %%
dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")

# %%
# This should fail all the times, because we clear the memory cache.
fully_cached_computation.clear_cache("mem")
dbg.dassert_eq(fully_cached_computation(*inputs), exp_output)
dbg.dassert_eq(fully_cached_computation.get_last_cache_accessed(), "mem")
