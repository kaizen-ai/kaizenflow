# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebook implements an interface proposal for placing orders via Talos API (REST).
#
# Example:
# https://github.com/talostrading/samples/blob/master/python/rfqsample/rfqsample/rest.py

# %%
# %load_ext autoreload
# %autoreload 2

import base64
import hashlib
import hmac
import logging

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# ## Functions

# %%
def calculate_signature(api_secret, parts):
    """
    A signature required for some types of GET and POST requests.

    Not required for historical data.
    """
    payload = "\n".join(parts)
    hash = hmac.new(
        api_secret.encode("ascii"), payload.encode("ascii"), hashlib.sha256
    )
    hash.hexdigest()
    signature = base64.urlsafe_b64encode(hash.digest()).decode()
    return signature

# %%
