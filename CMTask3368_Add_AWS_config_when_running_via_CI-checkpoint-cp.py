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
def _generate_config_txt(
    config: configparser.RawConfigParser, secret_keys: List
) -> str:
    """
    Create text for AWS config files.

    :param config: a parser that gets the AWS config file content
    :param secret_keys: the AWS credential keys that need to insert into a file
    :return: AWS crendials are formatted for ".aws/config" or ".aws/credentials"
        files
    """
    # ".aws/config" and ".aws/credentials" have different headers
    # for AWS profiles, so we check which secret keys contain ".aws/config"
    # key, then set the one is needed.
    if "region" in secret_keys:
        aws_profiles = ["profile am", "profile ck"]
    else:
        aws_profiles = ["am", "ck"]
    # Get all values for each secret key from the config. Sort them
    # by AWS profile.
    envs = {}
    for profile in aws_profiles:
        tmp = []
        for key in secret_keys:
            value = config.get(profile, key)
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
    return txt


def generate_aws_config() -> None:
    """
    Generate AWS config files with credentials.
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
# !cat .aws/config
