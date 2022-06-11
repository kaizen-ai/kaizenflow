"""
Import as:

import helpers.henv as henv
"""

import logging
import os
from typing import Any, List, Tuple, Union

import helpers.hgit as hgit
import helpers.hprint as hprint

# This module should not depend on any other helper or 3rd party modules.

_LOG = logging.getLogger(__name__)


# #############################################################################


_WARNING = "\033[33mWARNING\033[0m"


def has_module(module: str) -> bool:
    """
    Return whether a Python module can be imported or not.
    """
    code = f"""
try:
    import {module}
    has_module_ = True
except ImportError as e:
    print(_WARNING + ": " + str(e))
    has_module_ = False
"""
    # To make the linter happy.
    has_module_ = True
    exec(code, globals())
    return has_module_


# #############################################################################
# Print the env vars.
# #############################################################################


def get_env_var(env_name: str, as_bool: bool=False, default_value: Any = None,
                abort_on_missing: bool = True) -> Union[str, bool]:
    """
    Get an environment variable by name.

    :param env_name: name of the env var
    :param as_bool: convert the value into a Boolean
    :param default_value: the default value to use in case it's not defined
    :param abort_on_missing: if the env var is not defined aborts, otherwise use
        the default value
    :return: value of env var
    """
    if env_name not in os.environ:
        if abort_on_missing:
            assert 0, f"Can't find env var '{env_name}' in '{str(os.environ)}'"
        else:
            return default_value
    value = os.environ[env_name]
    if as_bool:
        # Convert the value into a boolean.
        if value in ("0", "", "None", "False"):
            value = False
        else:
            value = True
    return value


def get_env_vars() -> List[str]:
    """
    Return all the env vars that are expected to be set in Docker.
    """
    # Keep in sync with `lib_tasks.py:_generate_compose_file()`.
    env_var_names = [
        "AM_AWS_ACCESS_KEY_ID",
        "AM_AWS_DEFAULT_REGION",
        "AM_AWS_PROFILE",
        "AM_AWS_S3_BUCKET",
        "AM_AWS_SECRET_ACCESS_KEY",
        "AM_ECR_BASE_PATH",
        "AM_ENABLE_DIND",
        "AM_FORCE_TEST_FAIL",
        # The name of the host running Docker.
        "AM_HOST_NAME",
        # The OS of the host running Docker.
        "AM_HOST_OS_NAME",
        "AM_PUBLISH_NOTEBOOK_LOCAL_PATH",
        # Whether to check if certain property of the repo are as expected or not.
        "AM_REPO_CONFIG_CHECK",
        # Path to use for `repo_config.py`. E.g., used when running `dev_tools`
        # container to avoid using the `repo_config.py` corresponding to the
        # container launching the linter.
        "AM_REPO_CONFIG_PATH",
        "AM_TELEGRAM_TOKEN",
        "GH_ACTION_ACCESS_TOKEN",
        "CI",
    ]
    # No duplicates.
    assert len(set(env_var_names)) == len(
        env_var_names
    ), f"There are duplicates: {str(env_var_names)}"
    # Sort.
    env_var_names = sorted(env_var_names)
    return env_var_names


def get_secret_env_vars() -> List[str]:
    """
    Return the list of env vars that are secrets.
    """
    secret_env_var_names = [
        "AM_TELEGRAM_TOKEN",
        "AM_AWS_ACCESS_KEY_ID",
        "AM_AWS_SECRET_ACCESS_KEY",
        "GH_ACTION_ACCESS_TOKEN",
    ]
    # No duplicates.
    assert len(set(secret_env_var_names)) == len(
        secret_env_var_names
    ), f"There are duplicates: {str(secret_env_var_names)}"
    # Secret env vars are a subset of the env vars.
    env_vars = get_env_vars()
    if not set(secret_env_var_names).issubset(set(env_vars)):
        diff = set(secret_env_var_names).difference(set(env_vars))
        print(f"Secret vars in `{str(diff)} are not in '{str(env_vars)}'")
        assert 0
    # Sort.
    secret_env_var_names = sorted(secret_env_var_names)
    return secret_env_var_names


def check_env_vars() -> None:
    """
    Make sure all the expected env vars are defined.
    """
    env_vars = get_env_vars()
    for env_var in env_vars:
        assert (
            env_var in os.environ
        ), f"env_var='{str(env_var)}' is not in env_vars='{str(os.environ.keys())}''"


def env_vars_to_string() -> str:
    """
    Return a string with the signature of all the expected env vars (including the
    secret ones).
    """
    msg = []
    # Get the expected env vars and the secret ones.
    env_vars = get_env_vars()
    secret_env_vars = get_secret_env_vars()
    # Print a signature.
    for env_name in env_vars:
        is_defined = env_name in os.environ
        is_empty = is_defined and os.environ[env_name] == ""
        if not is_defined:
            msg.append(f"{env_name}=undef")
        else:
            if env_name in secret_env_vars:
                # Secret env var: print if it's empty or not.
                if is_empty:
                    msg.append(f"{env_name}=empty")
                else:
                    msg.append(f"{env_name}=***")
            else:
                # Not a secret var: print the value.
                msg.append(f"{env_name}='{os.environ[env_name]}'")
    msg = "\n".join(msg)
    return msg


def env_to_str() -> str:
    msg = ""
    #
    msg += "# Repo config:\n"
    msg += hprint.indent(hgit.execute_repo_config_code("config_func_to_str()"))
    msg += "\n"
    # System signature.
    msg += "# System signature:\n"
    msg += hprint.indent(get_system_signature()[0])
    msg += "\n"
    # Check which env vars are defined.
    msg += "# Env vars:\n"
    msg += hprint.indent(env_vars_to_string())
    msg += "\n"
    return msg


# #############################################################################
# Print the library versions.
# #############################################################################


def _get_library_version(lib_name: str) -> str:
    try:
        cmd = f"import {lib_name}"
        # pylint: disable=exec-used
        exec(cmd)
    except ImportError:
        version = "?"
    else:
        cmd = f"{lib_name}.__version__"
        version = eval(cmd)
    return version


def _append(
    txt: List[str], to_add: List[str], num_spaces: int = 4
) -> Tuple[List[str], List[str]]:
    txt.extend(
        [
            " " * num_spaces + line
            for txt_tmp in to_add
            for line in txt_tmp.split("\n")
        ]
    )
    to_add: List[str] = []
    return txt, to_add


def get_system_signature(git_commit_type: str = "all") -> Tuple[str, int]:
    # We use dynamic imports to minimize the dependencies.
    import helpers.hsystem as hsystem
    import helpers.hversion as hversio

    # TODO(gp): This should return a string that we append to the rest.
    container_dir_name = "."
    hversio.check_version(container_dir_name)
    #
    txt: List[str] = []
    # Add git signature.
    txt.append("# Git")
    txt_tmp: List[str] = []
    try:
        cmd = "git branch --show-current"
        _, branch_name = hsystem.system_to_one_line(cmd)
        txt_tmp.append(f"branch_name='{branch_name}'")
        #
        cmd = "git rev-parse --short HEAD"
        _, hash_ = hsystem.system_to_one_line(cmd)
        txt_tmp.append(f"hash='{hash_}'")
        #
        num_commits = 3
        if git_commit_type == "all":
            txt_tmp.append("# Last commits:")
            log_txt = hgit.git_log(num_commits=num_commits, my_commits=False)
            txt_tmp.append(hprint.indent(log_txt))
        elif git_commit_type == "mine":
            txt_tmp.append("# Your last commits:")
            log_txt = hgit.git_log(num_commits=num_commits, my_commits=True)
            txt_tmp.append(hprint.indent(log_txt))
        elif git_commit_type == "none":
            pass
        else:
            raise ValueError(f"Invalid value='{git_commit_type}'")
    except RuntimeError as e:
        _LOG.error(str(e))
    txt, txt_tmp = _append(txt, txt_tmp)
    # Add processor info.
    txt.append("# Machine info")
    txt_tmp: List[str] = []
    import platform

    uname = platform.uname()
    txt_tmp.append(f"system={uname.system}")
    txt_tmp.append(f"node name={uname.node}")
    txt_tmp.append(f"release={uname.release}")
    txt_tmp.append(f"version={uname.version}")
    txt_tmp.append(f"machine={uname.machine}")
    txt_tmp.append(f"processor={uname.processor}")
    try:
        import psutil

        has_psutil = True
    except ModuleNotFoundError as e:
        print(e)
        has_psutil = False
    if has_psutil:
        txt_tmp.append(f"cpu count={psutil.cpu_count()}")
        txt_tmp.append(f"cpu freq={str(psutil.cpu_freq())}")
        # TODO(gp): Report in MB or GB.
        txt_tmp.append(f"memory={str(psutil.virtual_memory())}")
        txt_tmp.append(f"disk usage={str(psutil.disk_usage('/'))}")
        txt, txt_tmp = _append(txt, txt_tmp)
    # Add package info.
    txt.append("# Packages")
    packages = []
    packages.append(("python", platform.python_version()))
    # import sys
    # print(sys.version)
    libs = [
        "gluonnlp",
        "gluonts",
        "joblib",
        "mxnet",
        "numpy",
        "pandas",
        "pyarrow",
        "scipy",
        "seaborn",
        "sklearn",
        "statsmodels",
    ]
    libs = sorted(libs)
    failed_imports = 0
    for lib in libs:
        version = _get_library_version(lib)
        if version.startswith("ERROR"):
            failed_imports += 1
        packages.append((lib, version))
    txt_tmp.extend([f"{l}: {v}" for (l, v) in packages])
    txt, txt_tmp = _append(txt, txt_tmp)
    #
    txt = "\n".join(txt)
    return txt, failed_imports
