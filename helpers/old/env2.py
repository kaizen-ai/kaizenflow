import logging
import os
from typing import Tuple

import helpers.io_ as hio
import helpers.old.conda as hocond
import helpers.printing as hprint
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)


# #############################################################################


def get_system_info(add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += hprint.frame("System info") + "\n"
    msg += "user name=%s\n" % hsyste.get_user_name()
    msg += "server name=%s\n" % hsyste.get_server_name()
    msg += "os name=%s\n" % hsyste.get_os_name()
    msg += "conda path=%s\n" % hocond.get_conda_path()
    msg += "conda env root=%s\n" % str(hocond.get_conda_envs_dirs())
    return msg


def get_package_summary(conda_env_name: str, add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += hprint.frame("Package summary") + "\n"
    conda_list = hocond.get_conda_list(conda_env_name)
    msg = ""
    for package in ["pandas", "numpy", "scipy", "arrow-cpp"]:
        ver = conda_list[package]["version"] if package in conda_list else "None"
        line = "%s: %s" % (package, ver)
        msg += line + "\n"
    return msg


def get_conda_export_list(conda_env_name: str, add_frame: bool) -> str:
    msg = ""
    if add_frame:
        msg += hprint.frame("Package summary") + "\n"
    cmd = (
        "(conda activate %s 2>&1 >/dev/null) && conda list --export"
        % conda_env_name
    )
    _, msg_tmp = hocond.conda_system_to_string(cmd)
    msg += msg_tmp
    return msg


def save_env_file(conda_env_name: str, dir_name: str) -> Tuple[str, str]:
    msg = ""
    msg += get_system_info(add_frame=True)
    msg += get_package_summary(conda_env_name, add_frame=True)
    msg += get_conda_export_list(conda_env_name, add_frame=True)
    # Save results.
    if dir_name is not None:
        file_name = "%s.%s.%s.%s.txt" % (
            conda_env_name,
            hsyste.get_user_name(),
            hsyste.get_os_name(),
            hsyste.get_server_name(),
        )
        dst_file = os.path.join(dir_name, file_name)
        dst_file = os.path.abspath(dst_file)
        hio.create_enclosing_dir(dst_file, incremental=True)
        _LOG.info("Saving conda env signature to '%s'", dst_file)
        hio.to_file(dst_file, msg)
    else:
        dst_file = None
    return msg, dst_file
