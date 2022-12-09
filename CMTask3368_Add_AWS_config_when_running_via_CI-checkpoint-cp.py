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
import os

import helpers.hs3 as hs3

# %%
creds = []
for profile in aws_profiles:
    tmp = {profile: []}
    for k, v in zip(envs[profile].keys(), envs[profile].values()):
        creds.append("=".join([k, v]))
txt = "\n".join(creds)
print(txt)


# %%
def _get_credentials_txt(config) -> str:
    credentials_vars = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_s3_bucket",
    ]
    aws_profiles = ["am", "ck"]
    envs = {}
    for profile in aws_profiles:
        tmp = {
            profile: {},
        }
        for var in credentials_vars:
            config.get(profile, var)
            tmp[profile].update({var: v})
        envs.update(tmp)
    creds = []
    # TODO(Nina): Factor out this piece of code to make created
    # file look like as original one. Look the output in the cell above.
    for profile in aws_profiles:
        for k, v in zip(envs[profile].keys(), envs[profile].values()):
            creds.append("=".join([k, v]))
    txt = "\n".join(creds)
    return txt


def generate_aws_config():
    config_file_path = "~/.aws/config"
    credentials_file_path = "~/.aws/credentials"
    if os.path.exists(config_file_path) or os.path.exists(credentials_file_path):
        return
    # Create files using the env vars as in `get_aws_credentials`
    # Get credentials values to fill "~/.aws/credentials" file.
    config_file_path.split("/")[-1]
    # Here will be a call of a function to create txt for config.
    #
    # Get credentials values to fill "~/.aws/credentials" file.
    credentials_file_name = credentials_file_path.split("/")[-1]
    credentials_config = hs3._get_aws_config(credentials_file_name)
    _get_credentials_txt(credentials_config)
    # Here will be a creation of two files.


# %%
