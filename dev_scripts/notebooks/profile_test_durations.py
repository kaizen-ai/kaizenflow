# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# Profile tests durations for test type.

# %% [markdown]
# # Imports

# %%
import logging
import re

import matplotlib.pyplot as plt

import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# `test_type` can be "fast_tests", "slow_tests", "superslow_tests",
# and "fast_slow_tests".

config = {"test_type": "fast_slow_tests"}


# %% [markdown]
# # Functions

# %%
def get_profiling_command(test_type: str):
    hdbg.dassert_in(
        test_type,
        ["fast_tests", "slow_tests", "superslow_tests", "fast_slow_tests"],
    )
    command = f"invoke run_{test_type} -p 'dev_scripts --durations 0' 2>&1 | tee tmp.{test_type}_profile.txt"
    return command


# %% [markdown]
# # Profile

# %%
print(get_profiling_command(config["test_type"]))

# %% [markdown]
# You need to post this command to the terminal and wait for the tests to pass.

# %%
with open(f"/app/tmp.{config['test_type']}_profile.txt") as fin:
    test_output = fin.read()

# %%
print(test_output)

# %% [markdown]
# Let's profile only calls, ignoring setups and teardowns.

# %%
durations = re.findall("\n  ==> (.*)s call", test_output)
durations = [float(duration) for duration in durations]
durations

# %% run_control={"marked": true}
plt.hist(durations)
_ = plt.title(f"Durations of {config['test_type']} in seconds")

# %%
