"""
Import as:

import helpers.version as hversi
"""

# This file should depend only on Python standard package since it's used by
# helpers/dbg.py, which is used everywhere.

import logging
import os
from typing import Optional

_LOG = logging.getLogger(__name__)


def get_code_version() -> str:
    """
    Return the code version.
    """
    _CODE_VERSION = "1.0.0"
    return _CODE_VERSION


# True if we are running inside a Docker container or inside GitHub Action.
IS_INSIDE_CONTAINER = os.path.exists("/.dockerenv") or ("CI" in os.environ)


def get_container_version() -> Optional[str]:
    """
    Return the container version.
    """
    if IS_INSIDE_CONTAINER:
        # We are running inside a container.
        # Keep the code and the container in sync by versioning both and requiring
        # to be the same.
        container_version = os.environ["CONTAINER_VERSION"]
    else:
        container_version = None
    return container_version


def _check_version(code_version: str, container_version: str) -> None:
    # We are running inside a container.
    # Keep the code and the container in sync by versioning both and requiring
    # to be the same.
    if container_version != code_version:
        msg = f"""
This code is not in sync with the container:
code_version={code_version} != container_version={container_version}")
You need to:
- merge origin/master into your branch with `invoke git_merge_origin_master`
- pull the latest container with `invoke docker_pull`
"""
        msg = msg.rstrip().lstrip()
        _LOG.error(msg)
        raise RuntimeError(msg)


def check_version() -> None:
    """
    Check that the code and container code have compatible version, otherwise
    raises `RuntimeError`.
    """
    # Get code version.
    code_version = get_code_version()
    # Get container version.
    env_var = "CONTAINER_VERSION"
    if env_var not in os.environ:
        container_version = None
        if IS_INSIDE_CONTAINER:
            # This situation happens when GH Actions pull the image using invoke
            # inside their container (but not inside ours), thus there is no
            # CONTAINER_VERSION.
            _LOG.warning("The env var should be defined when running inside a"
                " container", env_var)
    else:
        container_version = os.environ[env_var]
    print(f"code_version={code_version}, "
            f"container_version={container_version}, "
            f"inside_container={IS_INSIDE_CONTAINER}")
    if container_version is None:
        # No need to check.
        return
    _check_version(code_version, container_version)
