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

# Avoid dependency from other `helpers` modules to prevent import cycles.
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem

# Avoid dependency from other `helpers` modules, such as `helpers.henv`, to prevent
# import cycles.


_LOG = logging.getLogger(__name__)


_INFO = "\033[36mINFO\033[0m"
_WARNING = "\033[33mWARNING\033[0m"
_ERROR = "\033[31mERROR\033[0m"
#
_VERSION_RE = r"\d+\.\d+\.\d+"


# TODO(gp): -> full_system_config_to_str()
def env_to_str() -> str:
    """
    Return a string with the entire system configuration in terms of:

    - repo config
    - system signature
    - environment variables
    """
    msg = ""
    # Repo config.
    msg += "# Repo config:\n"
    msg_txt = f"repo_config path: {hgit.get_repo_config_file()}\n"
    msg_txt += hgit.execute_repo_config_code("config_func_to_str()") + "\n"
    msg += hprint.indent(msg_txt)
    msg += f"repo_config path: {hgit.get_repo_config_file()}\n"
    msg += hprint.indent(hgit.execute_repo_config_code("config_func_to_str()"))
    msg += "\n"
    # System signature.
    msg += "# System signature:\n"
    msg += hprint.indent(henv.get_system_signature()[0])
    msg += "\n"
    # Env vars.
    msg += "# Env vars:\n"
    msg += hprint.indent(henv.env_vars_to_string())
    msg += "\n"
    return msg


def check_version(container_dir_name: str) -> None:
    """
    Check that the code and container code have compatible version, otherwise
    raises `RuntimeError`.

    :param container_dir_name: container directory relative to the root directory
    """
    # TODO(gp): -> AM_SKIP_VERSION_CHECK.
    if "SKIP_VERSION_CHECK" in os.environ:
        # Skip the check altogether.
        return
    # Get code version.
    code_version = get_changelog_version(container_dir_name)
    container_version = _get_container_version()
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
    match = re.search(_VERSION_RE, changelog)
    if match:
        version = match.group()
    return version


def _get_container_version() -> Optional[str]:
    """
    Return the container version.

    :return: container code version from the env var
    """
    container_version: Optional[str] = None
    if hsystem.is_inside_docker():
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
            # Keep the code and the container in sync by versioning both and
            # requiring to be the same.
            container_version = os.environ["AM_CONTAINER_VERSION"]
    return container_version


def _check_version(code_version: str, container_version: str) -> bool:
    """
    Check whether the code version and the container version are the same.

    :param code_version: code version from the changelog
    :param container_version: container code version from the env var
    :return: whether the versions are the same or not
    """
    # Since the code version from the changelog is extracted with the
    # `_VERSION_RE` regex, we apply the same regex to the container version
    # to keep the representations comparable.
    match = re.search(_VERSION_RE, container_version)
    hdbg.dassert(
        match,
        (
            "Invalid format of the container code version '%s'; "
            "it should contain a number like '1.0.0'"
        ),
        container_version,
    )
    container_version = match.group()  # type: ignore
    # Check if the versions are the same.
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
