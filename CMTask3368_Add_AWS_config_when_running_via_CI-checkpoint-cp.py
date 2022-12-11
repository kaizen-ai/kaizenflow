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
l = ["[am]", "a", "b", "c", "[ck]", "d", "e", "f"]
for i in range(4, 9, 4):
    if i == 4:
        am = "\n".join(l[:i+1])
        print(am)
    else:
        ck = "\n".join(l[i:])
        print(ck)


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
    am_creds = "[am]\n".join(creds[:4])
    am_creds = "[ck]\n".join(creds[4:])
    txt = "\n\n".join([am_creds, ck8_creds])
    return txt


def generate_aws_config():
    config_file_path = "~/.aws/config"
    credentials_file_path = "~/.aws/credentials"
    if os.path.exists(config_file_path) or os.path.exists(credentials_file_path):
        return
    # Get config values to fill "~/.aws/credentials" file.
    config_file_name = config_file_path.split("/")[-1]
    config_vars = ["region"]
    # Here will be a call of a function to create txt for config.
    # It's going to be mostly the same as `_get_credentials_txt`
    # so need to share the common code or unify "_get_credentials_txt".
    #
    # Get credentials values to fill "~/.aws/credentials" file.
    credentials_file_name = credentials_file_path.split("/")[-1]
    credentials_aws_config = hs3._get_aws_config(credentials_file_name)
    txt = _get_credentials_txt(credentials_config)
    # Here will be a creation of two files.


# %%
