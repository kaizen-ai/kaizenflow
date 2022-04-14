"""
Import as:

import helpers.hversion as hversio
"""

# This code implements version control for code
# The code version is used in two circumstances:
# 1) when any code using dbg.py (which is included everywhere) starts in order to
#    verify that the running code and the container in which the code is running
#    are compatible
# 2) when a container is built to know what version of the code was used to build
#    it

# This file should depend only on Python standard package since it's used by
# helpers/dbg.py, which is used everywhere.

import logging
import os
import re
from typing import Optional, cast

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio

# Avoid dependency from other `helpers` modules, such as `helpers.henv`, to prevent
# import cycles.


_LOG = logging.getLogger(__name__)


_INFO = "\033[36mINFO\033[0m"
_WARNING = "\033[33mWARNING\033[0m"
_ERROR = "\033[31mERROR\033[0m"
#
_CHANGELOG_VERSION_RE = r"\d+\.\d+\.\d+"


def check_version(container_dir_name: str) -> None:
    """
    Check that the code and container code have compatible version, otherwise
    raises `RuntimeError`.

    :param container_dir_name: container directory relative to the root directory
    """
    if "SKIP_VERSION_CHECK" in os.environ:
        # Skip the check altogether.
        return
    # Get code version.
    code_version = get_changelog_version(container_dir_name)
    container_version = _get_container_version()
    is_inside_container = _is_inside_container()
    # Print information.
    is_inside_docker = _is_inside_docker()
    is_inside_ci = _is_inside_ci()
    msg = (
        f">>ENV<<: is_inside_container={is_inside_container}"
        f": code_version={code_version}"
        f", container_version={container_version}"
        f", is_inside_docker={is_inside_docker}"
        f", is_inside_ci={is_inside_ci}"
    )
    msg += (
        f", CI_defined={'CI' in os.environ}, CI='{os.environ.get('CI', 'nan')}'"
    )
    print(msg)
    # Check which env vars are defined.
    msg = ">>ENV<<:"
    for env_var in [
        "AM_AWS_PROFILE",
        "AM_ECR_BASE_PATH",
        "AM_S3_BUCKET",
        "AM_TELEGRAM_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_DEFAULT_REGION",
        "AWS_SECRET_ACCESS_KEY",
        "GH_ACTION_ACCESS_TOKEN",
    ]:
        msg += f" {env_var}={env_var in os.environ}"
    print(msg)
    # Check version, if possible.
    if container_version is None:
        # No need to check.
        return
    code_version = cast(str, code_version)
    _check_version(code_version, container_version)


def get_changelog_version(container_dir_name: str) -> Optional[str]:
    """
    Return latest version from changelog.txt file.

    :param container_dir_name: container directory relative to the root directory
    """
    version: Optional[str] = None
    supermodule = True
    root_dir = hgit.get_client_root(supermodule)
    # Note: for `amp` as submodule one should pass `container_dir_name` relative
    # to the root, e.g., `amp/optimizer` and not just `optimizer`.
    hdbg.dassert_ne(container_dir_name, "")
    changelog_file = os.path.join(root_dir, container_dir_name, "changelog.txt")
    hdbg.dassert_file_exists(changelog_file)
    changelog = hio.from_file(changelog_file)
    match = re.search(_CHANGELOG_VERSION_RE, changelog)
    if match:
        version = match.group()
    return version


def _get_container_version() -> Optional[str]:
    """
    Return the container version.
    """
    container_version: Optional[str] = None
    if _is_inside_container():
        env_var = "AM_CONTAINER_VERSION"
        if env_var not in os.environ:
            # This can happen when GH Actions pull the image using invoke
            # inside their container (but not inside ours), thus there is no
            # AM_CONTAINER_VERSION.
            print(
                _WARNING
                + f": The env var {env_var} should be defined when running inside a"
                " container"
            )
        else:
            # We are running inside a container.
            # Keep the code and the container in sync by versioning both and requiring
            # to be the same.
            container_version = os.environ["AM_CONTAINER_VERSION"]
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
        msg = f"\033[31m{msg}\033[0m"
        print(msg)
        # raise RuntimeError(msg)
    return is_ok


# TODO(gp): The circular dependency might be gone now.
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
    """
    if "CI" not in os.environ:
        ret = False
    else:
        ret = os.environ["CI"] != ""
    return ret


def _is_inside_container() -> bool:
    """
    Return whether we are running inside a Docker container or inside GitHub
    Action.
    """
    return _is_inside_docker() or _is_inside_ci()


# End copy.
