"""
This file contains info specific of `//amp` repo.
"""

import logging
import os
from typing import Dict, List

_LOG = logging.getLogger(__name__)
# We can't use `__file__` since this file is imported with an exec.
_LOG.debug("Importing //amp/repo_config.py")


def get_repo_map() -> Dict[str, str]:
    """
    Return a mapping of short repo name -> long repo name.
    """
    repo_map: Dict[str, str] = {"cm": "cryptokaizen/cmamp"}
    return repo_map


def get_extra_amp_repo_sym_name() -> str:
    return "cryptokaizen/cmamp"


# TODO(gp): -> get_gihub_host_name
def get_host_name() -> str:
    return "github.com"


def get_invalid_words() -> List[str]:
    return []


def get_docker_base_image_name() -> str:
    """
    Return a base name for docker image.
    """
    base_image_name = "cmamp"
    return base_image_name


# Copied from `system_interaction.py` to avoid circular imports.
def is_inside_ci() -> bool:
    """
    Return whether we are running inside the Continuous Integration flow.
    """
    if "CI" not in os.environ:
        ret = False
    else:
        ret = os.environ["CI"] != ""
    return ret


def run_docker_as_root() -> bool:
    """
    Return whether Docker should be run with root user.
    """
    # We want to run as user anytime we can.
    res = False
    if is_inside_ci():
        # When running as user in GH action we get an error:
        # ```
        # /home/.config/gh/config.yml: permission denied
        # ```
        # see https://github.com/alphamatic/amp/issues/1864
        # So we run as root in GH actions.
        res = True
    return res


def has_dind_support() -> bool:
    """
    Return whether this repo image supports Docker-in-Docker.
    """
    return True


def get_html_bucket_path() -> str:
    """
    Return the path to the bucket where published HTMLs are stored.
    """
    html_bucket = "cryptokaizen-html"
    # We do not use `os.path.join` since it converts `s3://` to `s3:/`.
    html_bucket_path = "s3://" + html_bucket
    return html_bucket_path
