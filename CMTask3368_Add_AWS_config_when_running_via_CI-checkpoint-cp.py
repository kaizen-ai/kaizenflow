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
def _generate_config_txt(config: configparser.RawConfigParser, secret_keys: List) -> str:
    """
    
    """
    if "region" in secret_keys:
        aws_profiles = ["profile am", "profile ck"]
    else:
        aws_profiles = ["am", "ck"]
    envs = {}
    for profile in aws_profiles:
        tmp = {
            profile: {},
        }
        for var in secret_keys:
            v = config.get(profile, var)
            tmp[profile].update({var: v})
        envs.update(tmp)
    creds = []
    for profile in aws_profiles:
        for k, v in zip(envs[profile].keys(), envs[profile].values()):
            creds.append("=".join([k, v]))
    idx = (len(creds) // 2)
    am_creds = "\n".join(creds[:idx])
    am_creds = f"[{aws_profiles[0]}]\n" + am_creds
    #
    ck_creds = "\n".join(creds[idx:])
    ck_creds = f"[{aws_profiles[1]}]\n" + ck_creds
    txt = "\n\n".join([am_creds, ck_creds])
    return txt


def generate_aws_config() -> None:
    """
    
    """
    config_file_path = ".aws/config"
    credentials_file_path = ".aws/credentials"
    # If file exists 
    if os.path.exists(config_file_path):
        return
    else:
        # Get config values to fill "~/.aws/config" file.
        file_name = config_file_path.split("/")[-1]
        credentials = hs3._get_aws_config(file_name)
        secret_keys = ["region"]
        txt = _generate_config_txt(credentials, secret_keys)
        # Create config file.
        hio.to_file(config_file_path, txt)
    if os.path.exists(credentials_file_path):
        return
    else:
        # Get credentials values to fill "~/.aws/credentials" file.
        file_name = credentials_file_path.split("/")[-1]
        secret_keys = [
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_s3_bucket",
        ]
        credentials = hs3._get_aws_config(file_name)
        txt = _generate_config_txt(credentials, secret_keys)    
        # Create credentials file.
        hio.to_file(credentials_file_path, txt)


# %%
generate_aws_config()

# %%
