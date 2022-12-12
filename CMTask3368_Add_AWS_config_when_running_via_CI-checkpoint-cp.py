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

import helpers.hio as hio
import helpers.hs3 as hs3

# %%
# Set missing env vars to show how generated file looks like.
os.environ["CK_AWS_ACCESS_KEY_ID"] = "$CK_AWS_ACCESS_KEY_ID"
os.environ["CK_AWS_SECRET_ACCESS_KEY"] = "$CK_AWS_SECRET_ACCESS_KEY"
os.environ["AM_AWS_ACCESS_KEY_ID"] = "$AM_AWS_ACCESS_KEY_ID"
os.environ["AM_AWS_SECRET_ACCESS_KEY"] = "$AM_AWS_SECRET_ACCESS_KEY"


# %%
def _dassert_all_env_vars_set(key_to_env_var: Dict[str, str]) -> None:
    for v in key_to_env_var.values():
        hdbg.dassert_in(v, os.environ)
        hdbg.dassert_ne(v, "")
        
        
def _get_aws_file_text(aws_profile: str, key_to_env_var: Dict[str, str]) -> List[str]:
    txt = [f"profile {aws_profile}\n"]
    for k, v in key_to_env_var.items(): 
        line = f"{k}={os.environ[v]}"
        txt.append(line)
    return txt

        
def _get_aws_config_text(aws_profile: str) -> List[str]:
    profile_prefix = aws_profile.upper()
    region_env_var = f"{profile_prefix}_AWS_DEFAULT_REGION"
    key_to_env_var = {
        "region": region_env_var
    }
    _dassert_all_env_vars_set(key_to_env_var)
    text = _get_aws_config_text(aws_profile, key_to_env_var)
    return text


def _get_aws_credentials_text(aws_profile: str) -> List[str]:
    profile_prefix = aws_profile.upper()
    key_to_env_var = {
        "aws_access_key_id": f"{profile_prefix}_AWS_ACCESS_KEY_ID",
        "aws_secret_access_key": f"{profile_prefix}_AWS_SECRET_ACCESS_KEY",
        "aws_s3_bucket": f"{profile_prefix}_AWS_S3_BUCKET",
    }
    _dassert_all_env_vars_set(key_to_env_var)
    text = _get_aws_config_text(aws_profile, key_to_env_var)
    return text


def generate_aws_files() -> None:
    # TODO(Grisha): maybe pass `home_dir` as a param?
    config_file_name = "~/.aws/config"
    credentials_file_name = "~/.aws/credentials"
    if os.path.exists(credentials_file_name) or os.path.exists(config_file_name)
        # Ensure that both files exist.
        _LOG.info("Both files exist: %s and %s; exiting", credentials_file_name, config_file_name)
    #
    aws_profiles = ["ck", "am"]
    config_file_text = []
    credentials_file_text = []
    for profile in aws_profiles:
        current_config_text = _get_aws_config_text(profile)
        config_file_text.append(current_config_text)
        current_credentials_text = _get_aws_credentials_text(profile)
        credentials_file_text.append(current_config_text)
    #
    config_file_text = "\n".join(config_file_text)
    hio.to_file(config_file_text, config_file_name)
    _LOG.debug("Saved AWS config to %s", config_file_name)
    #
    credentials_file_text = "\n".join(credentials_file_text)
    hio.to_file(credentials_file_text, credentials_file_name)
    _LOG.debug("Saved AWS credentials to %s", credentials_file_name)


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
# !cat ~/.aws/credentials

# %%
# !cat ~/.aws/config

# %%
# !cat .aws/credentials

# %%
# !cat .aws/config
