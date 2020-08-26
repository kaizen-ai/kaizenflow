import logging
import os
import platform
from typing import List

import helpers.conda as hco
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Merge env in system_interaction or conda.py? Or split the functions.


def _get_version(lib_name: str) -> str:
    try:
        cmd = "import %s" % lib_name
        # pylint: disable=exec-used
        exec(cmd)
    except ImportError:
        version = "ERROR: can't import"
    else:
        cmd = "%s.__version__" % lib_name
        version = eval(cmd)
    return version


def get_system_signature(git_commit_type: str = "all") -> (List[str], int):
    txt = []
    txt.append("# Packages")
    # Add package info.
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
        version = _get_version(lib)
        if version.startswith("ERROR"):
            failed_imports += 1
        packages.append((lib, version))
    txt.extend(["%15s: %s" % (l, v) for (l, v) in packages])
    # Add git signature.
    if git_commit_type == "all":
        log_txt = git.git_log(num_commits=3, my_commits=False)
        txt.append("# Last commits:")
        txt.append(pri.space(log_txt))
    elif git_commit_type == "mine":
        log_txt = git.git_log(num_commits=3, my_commits=False)
        txt.append("# Your last commits:")
        txt.append(pri.space(log_txt))
    elif git_commit_type == "none":
        pass
    else:
        raise ValueError("Invalid value='%s'" % git_commit_type)
    #
    txt = "\n".join(txt)
    return txt, failed_imports


# #############################################################################


def get_system_info(add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += pri.frame("System info") + "\n"
    msg += "user name=%s\n" % si.get_user_name()
    msg += "server name=%s\n" % si.get_server_name()
    msg += "os name=%s\n" % si.get_os_name()
    msg += "conda path=%s\n" % hco.get_conda_path()
    msg += "conda env root=%s\n" % str(hco.get_conda_envs_dirs())
    return msg


def get_package_summary(conda_env_name: str, add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += pri.frame("Package summary") + "\n"
    conda_list = hco.get_conda_list(conda_env_name)
    msg = ""
    for package in ["pandas", "numpy", "scipy", "arrow-cpp"]:
        ver = conda_list[package]["version"] if package in conda_list else "None"
        line = "%s: %s" % (package, ver)
        msg += line + "\n"
    return msg


def get_conda_export_list(conda_env_name: str, add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += pri.frame("Package summary") + "\n"
    cmd = (
        "(conda activate %s 2>&1 >/dev/null) && conda list --export"
        % conda_env_name
    )
    _, msg_tmp = hco.conda_system_to_string(cmd)
    msg += msg_tmp
    return msg


def save_env_file(conda_env_name: str, dir_name: str) -> (str, str):
    msg = ""
    msg += get_system_info(add_frame=True)
    msg += get_package_summary(conda_env_name, add_frame=True)
    msg += get_conda_export_list(conda_env_name, add_frame=True)
    # Save results.
    if dir_name is not None:
        file_name = "%s.%s.%s.%s.txt" % (
            conda_env_name,
            si.get_user_name(),
            si.get_os_name(),
            si.get_server_name(),
        )
        dst_file = os.path.join(dir_name, file_name)
        dst_file = os.path.abspath(dst_file)
        io_.create_enclosing_dir(dst_file, incremental=True)
        _LOG.info("Saving conda env signature to '%s'", dst_file)
        io_.to_file(dst_file, msg)
    else:
        dst_file = None
    return msg, dst_file
