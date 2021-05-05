"""
Import as:

import helpers.version as hversi
"""

# This file should depend only on Python standard package since it's used by
# helpers/dbg.py, which is used everywhere.

import logging
import os
import re
from typing import Optional

_LOG = logging.getLogger(__name__)


_INFO = "\033[36mINFO\033[0m"
_WARNING = "\033[33mWARNING\033[0m"
_ERROR = "\033[31mERROR\033[0m"


def check_version(dir_name: Optional[str] = None) -> None:
    """
    Check that the code and container code have compatible version, otherwise
    raises `RuntimeError`.
    """
    # Get code version.
    code_version = get_code_version(dir_name)
    is_inside_container = _is_inside_container()
    # Get container version.
    # TODO(gp): Use _get_container_version().
    env_var = "CONTAINER_VERSION"
    if env_var not in os.environ:
        container_version = None
        if is_inside_container:
            # This situation happens when GH Actions pull the image using invoke
            # inside their container (but not inside ours), thus there is no
            # CONTAINER_VERSION.
            print(
                _WARNING
                + f": The env var {env_var} should be defined when running inside a"
                " container"
            )
    else:
        container_version = os.environ[env_var]
    # Print information.
    is_inside_docker = _is_inside_docker()
    is_inside_ci = _is_inside_ci()
    msg = (
        f"is_inside_container={is_inside_container}"
        f": code_version={code_version}"
        f", container_version={container_version}"
        f", is_inside_docker={is_inside_docker}"
        f", is_inside_ci={is_inside_ci}"
    )
    if is_inside_container:
        print(msg)
    else:
        _LOG.debug("%s", msg)
    # Check version, if possible.
    if container_version is None:
        # No need to check.
        return
    _check_version(code_version, container_version)


def get_code_version(dir_name: Optional[str] = None) -> str:
    """
    Return the code version.
    """
    dir_name = dir_name or "."
    # Load the version.
    file_name = os.path.join(dir_name, "version.txt")
    file_name = os.path.abspath(file_name)
    assert os.path.exists(file_name), "Can't find file '%s'" % file_name
    with open(file_name) as f:
        version = f.readline().rstrip()
    # E.g., amp-1.0.0
    assert re.match(
        r"^\S+-\d+\.\d+\.\d+$", version
    ), "Invalid version '%s' from %s" % (version, file_name)
    return version


# Copied from helpers/system_interaction.py to avoid introducing dependencies.
def _is_inside_docker() -> bool:
    """
    Return whether we are inside a Docker container or not.
    """
    # From https://stackoverflow.com/questions/23513045
    return os.path.exists("/.dockerenv")


def _is_inside_ci() -> bool:
    """
    Return whether we are running inside the Continuous Integration flow.

    Note that this function returns:
    - True when we are running in GitHub system but not in our
      container (e.g., when we are inside an `invoke` workflow)
    - False once we enter our containers, since we don't propagate the `CI` env
      var through Docker compose
    """
    return "CI" in os.environ


def _is_inside_container() -> bool:
    """
    Return whether we are running inside a Docker container or inside GitHub
    Action.
    """
    return _is_inside_docker() or _is_inside_ci()


# End copy.


def _get_container_version() -> Optional[str]:
    """
    Return the container version.
    """
    if _is_inside_container():
        # We are running inside a container.
        # Keep the code and the container in sync by versioning both and requiring
        # to be the same.
        container_version = os.environ["CONTAINER_VERSION"]
    else:
        container_version = None
    return container_version


def _check_version(code_version: str, container_version: str) -> bool:
    # We are running inside a container.
    # Keep the code and the container in sync by versioning both and requiring
    # to be the same.
    is_ok = container_version == code_version
    if not is_ok:
        msg = f"""
-----------------------------------------------------------------------------
This code is not in sync with the container:
code_version='{code_version}' != container_version='{container_version}'
-----------------------------------------------------------------------------
You need to:
- merge origin/master into your branch with `invoke git_merge_master`
- pull the latest container with `invoke docker_pull`
"""
        msg = msg.rstrip().lstrip()
        msg = "\033[31m%s\033[0m" % msg
        print(msg)
        # raise RuntimeError(msg)
    return is_ok
