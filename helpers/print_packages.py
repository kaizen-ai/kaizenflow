#!/usr/bin/env python

import argparse
import logging
import os
import pprint

import helpers.conda as hco
import helpers.dbg as dbg
import helpers.helper_io as io_
import helpers.printing as print_
import helpers.system_interaction as hsi

_LOG = logging.getLogger(__name__)



def get_system_info(add_frame):
    msg = ""
    if add_frame:
        msg += print_.frame("System info") + "\n"
    msg += "user name=%s\n" % hsi.get_user_name()
    msg += "server name=%s\n" % hsi.get_server_name()
    msg += "os name=%s\n" % hsi.get_os_name()
    msg += "conda path=%s\n" % hco.get_conda_path()
    msg += "conda env root=%s\n" % str(hco.get_conda_envs_dirs())
    return msg


def get_package_summary(conda_env_name, add_frame):
    msg = ""
    if add_frame:
        msg += print_.frame("Package summary") + "\n"
    conda_list = hco.get_conda_list(conda_env_name)
    msg = ""
    for package in ["pandas", "numpy", "scipy", "arrow-cpp"]:
        ver = conda_list[package]["version"] if package in conda_list else "None"
        line = "%s: %s" % (package, ver)
        msg += line + "\n"
    return msg


def get_conda_export_list(conda_env_name, add_frame):
    msg = ""
    if add_frame:
        msg += print_.frame("Package summary") + "\n"
    cmd = "(conda activate %s 2>&1 >/dev/null) && conda list --export" % conda_env_name
    _, msg_tmp = hco.conda_system_to_string(cmd)
    msg += msg_tmp
    return msg


def save_env_file(conda_env_name, dir_name):
    msg = ""
    msg += get_system_info(add_frame=True)
    msg += get_package_summary(conda_env_name, add_frame=True)
    msg += get_conda_export_list(conda_env_name, add_frame=True)
    # Save results.
    if dir_name is not None:
        file_name = "%s.%s.%s.%s.txt" % (conda_env_name, hsi.USER_NAME,
                                         hsi.OS_NAME, hsi.SERVER_NAME)
        dst_file = os.path.join(dir_name, file_name)
        dst_file = os.path.abspath(dst_file)
        io_.create_enclosing_dir(dst_file, incremental=True)
        _LOG.info("Saving conda env signature to '%s'", dst_file)
        io_.to_file(dst_file, msg)
    return msg, dst_file


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    parser.add_argument(
        "--conda_env_name",
        help="Environment name",
        default="develop",
        type=str)
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    msg = save_env_file(args.conda_env_name, dir_name=None)
    print(msg)


if __name__ == '__main__':
    _main()
