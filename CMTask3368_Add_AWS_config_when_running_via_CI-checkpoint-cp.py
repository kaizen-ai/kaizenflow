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
import logging
import os
from typing import Dict, List, Optional

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hio as hio
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# Set missing env vars to show how generated file looks like.
os.environ["CK_AWS_ACCESS_KEY_ID"] = "$CK_AWS_ACCESS_KEY_ID"
os.environ["CK_AWS_SECRET_ACCESS_KEY"] = "$CK_AWS_SECRET_ACCESS_KEY"
os.environ["AM_AWS_ACCESS_KEY_ID"] = "$AM_AWS_ACCESS_KEY_ID"
os.environ["AM_AWS_SECRET_ACCESS_KEY"] = "$AM_AWS_SECRET_ACCESS_KEY"
os.environ["AM_AWS_DEFAULT_REGION"] = "$AM_AWS_DEFAULT_REGION"
os.environ["CK_AWS_DEFAULT_REGION"] = "$CK_AWS_DEFAULT_REGION"


# %%
def _dassert_all_env_vars_set(key_to_env_var: Dict[str, str]) -> None:
    """
    Assert if variable is not set to the env and equal to an empty string.
    """
    for v in key_to_env_var.values():
        hdbg.dassert_in(v, os.environ)
        hdbg.dassert_ne(v, "")


def _get_aws_file_text(key_to_env_var: Dict[str, str]) -> List[str]:
    """
    Generate text from env vars for AWS files.

    E.g.:
    ```
    aws_access_key_id=***
    aws_secret_access_key=***
    aws_s3_bucket=***
    ```

    :param key_to_env_var: variables to get from the env
    :return: AWS file text
    """
    txt = []
    for k, v in key_to_env_var.items():
        line = f"{k}={os.environ[v]}"
        txt.append(line)
    return txt


def _get_aws_config_text(aws_profile: str) -> str:
    """
    Generate text for the AWS config file, i.e. ".aws/config".
    """
    # Set which env vars we need to get.
    profile_prefix = aws_profile.upper()
    region_env_var = f"{profile_prefix}_AWS_DEFAULT_REGION"
    key_to_env_var = {"region": region_env_var}
    # Check that env var is set.
    _dassert_all_env_vars_set(key_to_env_var)
    text = _get_aws_file_text(key_to_env_var)
    text.insert(0, f"[profile {aws_profile}]")
    text = "\n".join(text)
    return text


def _get_aws_credentials_text(aws_profile: str) -> str:
    """
    Generate text for the AWS credentials file, i.e. ".aws/credentials".
    """
    # Set which env vars we need to get.
    profile_prefix = aws_profile.upper()
    key_to_env_var = {
        "aws_access_key_id": f"{profile_prefix}_AWS_ACCESS_KEY_ID",
        "aws_secret_access_key": f"{profile_prefix}_AWS_SECRET_ACCESS_KEY",
        "aws_s3_bucket": f"{profile_prefix}_AWS_S3_BUCKET",
    }
    # Check that env var is set.
    _dassert_all_env_vars_set(key_to_env_var)
    text = _get_aws_file_text(key_to_env_var)
    text.insert(0, f"[{aws_profile}]")
    text = "\n".join(text)
    return text


def generate_aws_files(
    home_dir: str = "~",
    aws_profiles: Optional[List[str]] = None,
) -> None:
    """
    Generate AWS files with credentials.
    """
    config_file_name = f"{home_dir}/.aws/config"
    credentials_file_name = f"{home_dir}/.aws/credentials"
    if os.path.exists(credentials_file_name) or os.path.exists(config_file_name):
        # Ensure that both files exist.
        hdbg.dassert_file_exists(credentials_file_name)
        hdbg.dassert_file_exists(config_file_name)
        _LOG.info(
            "Both files exist: %s and %s; exiting",
            credentials_file_name,
            config_file_name,
        )
        return
    # Get text with credentials for both files.
    if aws_profiles is None:
        aws_profiles = ["am", "ck"]
    config_file_text = []
    credentials_file_text = []
    for profile in aws_profiles:
        current_config_text = _get_aws_config_text(profile)
        config_file_text.append(current_config_text)
        current_credentials_text = _get_aws_credentials_text(profile)
        credentials_file_text.append(current_credentials_text)
    # Create both files.
    config_file_text = "\n\n".join(config_file_text)
    hio.to_file(config_file_name, config_file_text)
    _LOG.debug("Saved AWS config to %s", config_file_name)
    #
    credentials_file_text = "\n\n".join(credentials_file_text)
    hio.to_file(credentials_file_name, credentials_file_text)
    _LOG.debug("Saved AWS credentials to %s", credentials_file_name)


# %%
generate_aws_files()

# %%
# !cat "~/.aws/config"

# %%
# !cat "~/.aws/credentials"
