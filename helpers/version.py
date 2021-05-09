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


# TODO(gp): It's not clear how to generalize this for different containers.
#  For `amp` makes sense to check at top of the repo.
def get_code_version(dir_name: Optional[str] = None) -> str:
    """
    Return the code version.

    The code version is used when:
    - the code starts in order to verify that the code and the containers are
      compatible
    - the container is built to know what version of the code was used

    :param dir_name: the path to the `version.txt` file. If None uses the dir
        one level up with respect to the this file (i.e., `amp` dir)
    """
    dir_name_was_specified = False
    if not dir_name:
        # Use the version one level up.
        dir_name = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.abspath(os.path.join(dir_name, ".."))
    else:
        dir_name_was_specified = True
    # Load the version.
    file_name = os.path.join(dir_name, "version.txt")
    file_name = os.path.abspath(file_name)
    version_file_exists = os.path.exists(file_name)
    if version_file_exists:
        with open(file_name) as f:
            version = f.readline().rstrip()
        # E.g., `amp-1.0.0`.
        assert re.match(
            r"^\S+-\d+\.\d+\.\d+$", version
        ), "Invalid version '%s' from %s" % (version, file_name)
    else:
        if dir_name_was_specified:
            # If the `dir_name` was specified, we expect to find the file.
            assert version_file_exists, "Can't find file '%s' for dir_name='%s'" % (
                file_name,
                dir_name,
            )
        else:
            # If the `dir_name` was not specified, then it's ok not to find the
            # file.
            print(
                _WARNING
                + ": Can't find versions.txt, so using version=None"
            )
            version = None
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
    container_version: Optional[str] = None
    if _is_inside_container():
        # We are running inside a container.
        # Keep the code and the container in sync by versioning both and requiring
        # to be the same.
        container_version = os.environ["CONTAINER_VERSION"]
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
