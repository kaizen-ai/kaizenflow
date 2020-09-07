# ---
# jupyter:
# jupytext:
# formats: ipynb,py:percent
# text_representation:
# extension: .py
# format_name: percent
# format_version: '1.3'
# jupytext_version: 1.5.2
# kernelspec:
# display_name: Python 3
# language: python
# name: python3
# ---

# %% [markdown] pycharm={"name": "#%% md\n"}
# # Test Cache in Jupyter Notebook

# %%
import joblib

print(joblib)

import helpers.cache as hcac

# %%
import helpers.dbg as dbg

# %% [markdown] pycharm={"name": "#%% md\n"}
# # Define computation function
#
# This function will be subjected to cache.

# %% pycharm={"name": "#%%\n"}
def computation_function(a, b):
    out = a + b
    print("Sum: %s + %s = %s" % (a, b, out))
    return out


inputs = (1, 2)
exp_output = 3

computation_function(*inputs)


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


# %% [markdown]
# ## LRU Cache

# %%
def hello():
    print("hello")


print(id(hello))

# %%
import functools

mem_func = functools.lru_cache(maxsize=None)(hello)

mem_func()

# %%
mem_func = functools.lru_cache(maxsize=None)(hello)

mem_func()


# %%
def hello():
    print("hello")


print(id(hello))


# %%
def hello():
    assert 0
    print("good bye")


print(id(hello))

# %%
# hello()
print(id(hello))
mem_func()

# %%
mem_func = functools.lru_cache(maxsize=None)(hello)

mem_func()

# %% [markdown]
# ## Joblib cache

# %%
import joblib

disk_cache = joblib.Memory("./joblib.cache", verbose=0, compress=1)
disk_cache.clear()


# %%
def hello():
    print("hello")


print(id(hello))

cached_hello = disk_cache.cache(hello)

print(id(cached_hello))

# %%
cached_hello()

# %%
cached_hello()


# %%
def hello():
    print("hello")


# %%
cached_hello()


# %%
def hello():
    print("good bye")


cached_hello = disk_cache.cache(hello)

# %%
cached_hello()


# %%
def hello():
    print("good bye")


cached_hello = disk_cache.cache(hello)

cached_hello()


# %% [markdown] pycharm={"name": "#%% md\n"}
# ## Memory cache

# %%
def computation_function(a, b):
    out = a + b
    print("Sum: %s + %s = %s" % (a, b, out))
    return out


inputs = (1, 2)
exp_output = 3

# %% pycharm={"name": "#%%\n"}
memory_cached_computation = hcac.Cached(
    computation_function, use_mem_cache=True, use_disk_cache=False
)

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
print(memory_cached_computation.get_last_cache_accessed())
dbg.dassert_eq(memory_cached_computation.get_last_cache_accessed(), "mem")

# %%
memory_cached_computation._func.__code__
memory_cached_computation._func.co_filename


# %%
computation_function.__code__

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

# %%
print(disk_cached_computation._execute_func_from_disk_cache.func)

from joblib.func_inspect import get_func_code

print(
    get_func_code(disk_cached_computation._execute_func_from_disk_cache.func)[0]
)
dbg.dassert_eq(disk_cached_computation(*inputs), exp_output)
dbg.dassert_eq(disk_cached_computation.get_last_cache_accessed(), "disk")

# %% [markdown]
# ## Full cache

# %% pycharm={"name": "#%%\n"}
fully_cached_computation = hcac.Cached(
    computation_function, use_mem_cache=True, use_disk_cache=True
)

fully_cached_computation.clear_cache("disk")

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
