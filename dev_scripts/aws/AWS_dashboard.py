# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python [conda env:develop] *
#     language: python
#     name: conda-env-develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import datetime
import logging
import os
import platform

import numpy as np
import pandas as pd
import scipy
import matplotlib
import matplotlib.pyplot as plt

import helpers.dbg as dbg
import helpers.printing as print_
import helpers.system_interaction as si

# %%
print_.config_notebook()

# TODO(gp): Changing level during the notebook execution doesn't work. Fix it.
#dbg.init_logger(verb=logging.DEBUG)
dbg.init_logger(verb=logging.INFO)

# %% [markdown]
# ## Describe-instances

# %%
cmd = "aws ec2 describe-instances"
_, txt = si.system_to_string(cmd)

# %%
data = json.loads(txt)

data

# %%
df = pd.io.json.json_normalize(data["Reservations"], record_path="Instances")
df_tmp = df["InstanceId InstanceType LaunchTime PublicIpAddress State".split()]

display(df_tmp)
#pd.io.json.json_normalize(data["Reservations"]["Instances"])

# %%
#help(pd.io.json.json_normalize)
