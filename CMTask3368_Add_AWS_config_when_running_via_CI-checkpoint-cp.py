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
import configparser
import os
from typing import List

import helpers.hio as hio
import helpers.hs3 as hs3

# %%
# Set missing env vars to show how generated file looks like.
os.environ["CK_AWS_ACCESS_KEY_ID"] = "$CK_AWS_ACCESS_KEY_ID"
os.environ["CK_AWS_SECRET_ACCESS_KEY"] = "$CK_AWS_SECRET_ACCESS_KEY"
os.environ["AM_AWS_ACCESS_KEY_ID"] = "$AM_AWS_ACCESS_KEY_ID"
os.environ["AM_AWS_SECRET_ACCESS_KEY"] = "$AM_AWS_SECRET_ACCESS_KEY"


# %%
def generate_aws_config() -> None:
    """
    Generate AWS config file with credentials.
    """
    credentials_file_path = ".aws/credentials"
    if os.path.exists(credentials_file_path):
        return
    else:
        # Get credentials values to fill "~/.aws/credentials" file.
        aws_profiles = ["AM", "CK"]
        secret_keys = [
                "_AWS_ACCESS_KEY_ID",
                "_AWS_SECRET_ACCESS_KEY",
                "_AWS_S3_BUCKET",
        ]
        # Get all values for each env var. Sort them
        # by AWS profile.
        envs = {}
        for profile in aws_profiles:
            tmp = []
            for key in secret_keys:
                # Add prefix to variable name, e.g., "CK_AWS_SECRET_ACCESS_KEY",
                # and get its value from the env.
                key = f"{profile}{key}"
                value = os.environ[key]
                secret = "=".join([key, value])
                tmp.append(secret)
            envs.update({profile: tmp})
        # Create text to insert into the config file.
        am_creds = "\n".join(envs[aws_profiles[0]])
        am_creds = f"[{aws_profiles[0]}]\n" + am_creds
        #
        ck_creds = "\n".join(envs[aws_profiles[1]])
        ck_creds = f"[{aws_profiles[1]}]\n" + ck_creds
        txt = "\n\n".join([am_creds, ck_creds])
        # Write to file.
        hio.to_file(credentials_file_path, txt)


# %%
generate_aws_config()

# %%
# !cat .aws/credentials
