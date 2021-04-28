import logging
import os

# This file should depend only on Python standard package since it's used by helpers/dbg.py
# which is used everywhere.

_LOG = logging.getLogger(__name__)

from typing import Any, Iterable, List, Optional, Tuple, Type, Union


def get_code_version() -> str:
    """"
    Return the code version.
    """
    _CODE_VERSION = "1.0.0"
    return _CODE_VERSION


def get_container_version() -> Optional[str]:
    """
    Return the container version.
    """
    if os.path.exists("/.dockerenv"):
        # We are running inside a container.
        # Keep the code and the container in sync by versioning both and requiring
        # to be the same.
        container_version = os.environ["CONTAINER_VERSION"]
    else:
        container_version = None
    return container_version


def check_version() -> None:
    """
    Check that the code and container code have compatible version, otherwise
    raises `RuntimeError`.
    """
    container_version = os.environ["CONTAINER_VERSION"]
    if container_version is None:
        return
    # We are running inside a container.
    # Keep the code and the container in sync by versioning both and requiring
    # to be the same.
    code_version = get_code_version()
    print("code_version={CODE_VERSION}, container_version={container_ver}")
    if container_version != code_version:
        msg = f"""
This code is not in sync with the container:
code_version={code_version} != container_version={container_version}")
You need to:
- merge origin/master into your branch with `invoke git_merge_origin_master`
- pull the latest container with `invoke docker_pull`
"""
        msg = msg.rsplit().lstrip()
        _LOG.error(msg)
        raise RuntimeError(msg)