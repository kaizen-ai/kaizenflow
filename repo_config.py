"""
Contain info specific of `//cmamp` repo.
"""

import functools
import logging
import os
from typing import Dict, List, Optional

_LOG = logging.getLogger(__name__)


def _print(msg: str) -> None:
    # _LOG.info(msg)
    print(msg)


# We can't use `__file__` since this file is imported with an exec.
# _print("Importing //cmamp/repo_config.py")


# #############################################################################
# Repo info.
# #############################################################################


def get_name() -> str:
    return "//cmamp"


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


# #############################################################################


# //cmamp runs on:
# - MacOS
#   - Supports Docker privileged mode
#   - The same user and group is used inside the container
#   - Root can also be used
# - Linux (dev server, GitHub CI)
#   - Supports Docker privileged mode
#   - The same user and group is used inside the container


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


def is_inside_docker() -> bool:
    """
    Return whether we are inside a container or not.
    """
    # From https://stackoverflow.com/questions/23513045
    return os.path.exists("/.dockerenv")


# End copy.

# We can't rely only on the name of the host to infer where we are running,
# since inside Docker the name of the host is like `01a7e34a82a5`. Of course,
# there is no way to know anything about the host for security reason, so we
# pass this value from the external environment to the container, through env
# vars (e.g., `AM_HOST_NAME`, `AM_HOST_OS_NAME`).


# pylint: disable=line-too-long
def is_dev_ck() -> bool:
    # sysname='Darwin'
    # nodename='gpmac.lan'
    # release='19.6.0'
    # version='Darwin Kernel Version 19.6.0: Mon Aug 31 22:12:52 PDT 2020; root:xnu-6153.141.2~1/RELEASE_X86_64'
    # machine='x86_64'
    host_name = os.uname()[1]
    host_names = ("dev1", "dev2")
    am_host_name = os.environ.get("AM_HOST_NAME")
    _LOG.debug("host_name=%s am_host_name=%s", host_name, am_host_name)
    is_dev_ck_ = host_name in host_names or am_host_name in host_names
    return is_dev_ck_


def is_dev4() -> bool:
    """
    Return whether it's running on dev4.
    """
    host_name = os.uname()[1]
    dev4 = "cf-spm-dev4"
    am_host_name = os.environ.get("AM_HOST_NAME")
    _LOG.debug("host_name=%s am_host_name=%s", host_name, am_host_name)
    is_dev4_ = dev4 in (host_name, am_host_name)
    return is_dev4_


def is_mac() -> bool:
    host_os_name = os.uname()[0]
    am_host_os_name = os.environ.get("AM_HOST_OS_NAME")
    _LOG.debug(
        "host_os_name=%s am_host_os_name=%s", host_os_name, am_host_os_name
    )
    is_mac_ = host_os_name == "Darwin" or am_host_os_name == "Darwin"
    return is_mac_


def _raise_invalid_host() -> None:
    host_os_name = os.uname()[0]
    am_host_os_name = os.environ.get("AM_HOST_OS_NAME")
    raise ValueError(
        f"Don't recognize host: host_os_name={host_os_name}, "
        f"am_host_os_name={am_host_os_name}"
    )


def enable_privileged_mode() -> bool:
    """
    Return whether an host supports privileged mode for its containers.
    """
    if get_name() == "//dev_tools":
        val = False
    else:
        if is_mac():
            val = True
        elif is_cmamp_prod():
            val = False
        elif is_dev_ck():
            val = True
        elif is_dev4():
            val = False
        elif is_inside_ci():
            val = True
        else:
            _raise_invalid_host()
    return val


def has_docker_sudo() -> bool:
    """
    Return whether commands should be run with sudo or not.
    """
    if is_dev_ck():
        val = False
    elif is_dev4():
        val = False
    elif is_inside_ci():
        val = False
    elif is_mac():
        val = True
    elif is_cmamp_prod():
        val = False
    else:
        _raise_invalid_host()
    return val


# TODO(gp): -> has_docker_privileged_mode
@functools.lru_cache()
def has_dind_support() -> bool:
    """
    Return whether the current container supports privileged mode.

    This is need to use Docker-in-Docker.
    """
    if not is_inside_docker():
        # Outside Docker there is no privileged mode.
        return False
    # TODO(gp): This part is not multi-process friendly. When multiple
    # processes try to run this code they interfere. A solution is to run `ip
    # link` in the entrypoint and create a has_docker_privileged_mode file
    # which contains the value.
    # return True
    # Thus we rely on the approach from https://stackoverflow.com/questions/32144575
    # checking if we can execute.
    # Sometimes there is some state left, so we need to clean it up.
    cmd = "ip link delete dummy0 >/dev/null 2>&1"
    if is_mac() or is_dev_ck():
        cmd = f"sudo {cmd}"
    os.system(cmd)
    #
    cmd = "ip link add dummy0 type dummy >/dev/null 2>&1"
    if is_mac() or is_dev_ck():
        cmd = f"sudo {cmd}"
    rc = os.system(cmd)
    has_dind = rc == 0
    # Clean up, after the fact.
    cmd = "ip link delete dummy0 >/dev/null 2>&1"
    if is_mac() or is_dev_ck():
        cmd = f"sudo {cmd}"
    rc = os.system(cmd)
    # dind is supported on both Mac and GH Actions.
    if True:
        if is_cmamp_prod():
            assert not has_dind, "Not expected privileged mode"
        elif get_name() == "//dev_tools":
            assert not has_dind, "Not expected privileged mode"
        else:
            if is_mac() or is_dev_ck() or is_inside_ci():
                assert has_dind, "Expected privileged mode"
            elif is_dev4():
                assert not has_dind, "Not expected privileged mode"
            else:
                _raise_invalid_host()
    return has_dind


def use_docker_sibling_containers() -> bool:
    """
    Return whether to use Docker sibling containers.
    """
    # TODO(gp): We should enable it for dev4.
    val = False
    return val


def get_shared_data_dirs() -> Optional[Dict[str, str]]:
    """
    Get path of dir storing data shared between different users on the host and
    Docker.

    E.g., one can mount a central dir `/data/shared`, shared by multiple
    users, on a dir `/shared_data` in Docker.
    """
    if is_dev4():
        shared_data_dirs = {"/local/home/share/cache": "/cache"}
    elif is_dev_ck():
        shared_data_dirs = {"/data/shared": "/shared_data"}
    elif is_mac() or is_inside_ci() or is_cmamp_prod():
        shared_data_dirs = None
    else:
        _raise_invalid_host()
    return shared_data_dirs


def use_docker_network_mode_host() -> bool:
    ret = bool(is_mac() or is_dev_ck())
    return ret


def run_docker_as_root() -> bool:
    """
    Return whether Docker should be run with root user.

    I.e., adding `--user $(id -u):$(id -g)` to docker compose or not.
    """
    if is_inside_ci():
        # When running as user in GH action we get an error:
        # ```
        # /home/.config/gh/config.yml: permission denied
        # ```
        # see https://github.com/alphamatic/amp/issues/1864
        # So we run as root in GH actions.
        res = True
    elif is_dev4():
        # //lime runs on a system with Docker remap which assumes we don't
        # specify user credentials.
        res = True
    elif is_dev_ck():
        # On dev1 / dev2 we run as users specifying the user / group id as
        # outside.
        res = False
    elif is_mac():
        res = False
    elif is_cmamp_prod():
        res = False
    else:
        _raise_invalid_host()
    return res


def get_docker_user() -> str:
    """
    Return the user that runs Docker, if any.
    """
    if is_dev4():
        val = "spm-sasm"
    else:
        val = ""
    return val


def get_html_bucket_path() -> str:
    """
    Return the path to the bucket where published HTMLs are stored.
    """
    html_bucket = "cryptokaizen-html"
    # We do not use `os.path.join` since it converts `s3://` to `s3:/`.
    html_bucket_path = "s3://" + html_bucket
    return html_bucket_path


def get_docker_shared_group() -> str:
    """
    Return the group of the user running Docker, if any.
    """
    if is_dev4():
        val = "spm-sasm-fileshare"
    else:
        val = ""
    return val


def skip_submodules_test() -> bool:
    """
    Return whether the tests in the submodules should be skipped.

    E.g. while running `i run_fast_tests`.
    """
    if get_name() == "//dev_tools":
        # Skip running `amp` tests from `dev_tools`.
        return True
    return False


# #############################################################################
# S3 buckets.
# #############################################################################


def is_AM_S3_available() -> bool:
    # AM bucket is always available.
    val = True
    _LOG.debug("val=%s", val)
    return val


def is_CK_S3_available() -> bool:
    # CK bucket is not available for `//lemonade` and `//amp` unless it's on
    # `dev_ck`.
    val = True
    if is_mac():
        val = False
    elif is_dev4():
        # CK bucket is not available on dev4.
        val = False
    elif is_inside_ci():
        if get_name() in ("//amp", "//dev_tools"):
            # No CK bucket.
            val = False
    _LOG.debug("val=%s", val)
    return val


def is_cmamp_prod() -> bool:
    """
    Detect whether this is a production container.
    This env var is set inside devops/docker_build/prod.Dockerfile.
    """
    return os.environ.get("CK_IN_PROD_CMAMP_CONTAINER", False)


# #############################################################################


def config_func_to_str() -> str:
    """
    Print the value of all the config functions.
    """
    ret: List[str] = []
    for func_name in sorted(
        [
            "enable_privileged_mode",
            "get_docker_base_image_name",
            "get_docker_user",
            "get_docker_shared_group",
            # "get_extra_amp_repo_sym_name",
            "get_host_name",
            "get_invalid_words",
            "get_name",
            "get_repo_map",
            "get_shared_data_dirs",
            "has_dind_support",
            "has_docker_sudo",
            "is_AM_S3_available",
            "is_CK_S3_available",
            "is_dev_ck",
            "is_dev4",
            "is_inside_ci",
            "is_inside_docker",
            "is_mac",
            "run_docker_as_root",
            "skip_submodules_test",
            "use_docker_sibling_containers",
            "use_docker_network_mode_host",
        ]
    ):
        try:
            _LOG.debug("func_name=%s", func_name)
            func_value = eval(f"{func_name}()")
        except NameError:
            func_value = "*undef*"
        msg = f"{func_name}='{func_value}'"
        ret.append(msg)
        # _print(msg)
    ret = "\n".join(ret)
    return ret


if False:
    print(config_func_to_str())
    # assert 0
