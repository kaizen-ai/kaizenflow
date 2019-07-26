#!/usr/bin/env python

# Note that this file must run with python2.7 to bootstrap conda.

import os
import subprocess
import sys

# TODO(gp): Not sure this is a good idea since it might create cyclic dependencies.
import helpers.dbg as dbg

# We cannot use system_interaction since it depends on python3, and this script is
# used to configure conda to use python3. So to break the cyclic dependency we inline
# the functions.
#import helpers.system_interaction as si


def _system_to_string(cmd):
    py_ver = sys.version_info[0]
    if py_ver == 2:
        txt = subprocess.check_output(cmd)
    elif py_ver == 3:
        txt = subprocess.getoutput(cmd)
    else:
        raise RuntimeError("Invalid py_ver=%s" % py_ver)
    txt = [f for f in txt.split("\n") if f]
    dbg.dassert_eq(len(txt), 1)
    return txt[0]


_USER_NAME = None


def _get_user_name():
    global _USER_NAME
    if _USER_NAME is None:
        import getpass
        _USER_NAME = getpass.getuser()
        # This seems to be flakey on some systems.
        #_USER_NAME = _system_to_string("whoami")
    return _USER_NAME


_SERVER_NAME = None


def _get_server_name():
    global _SERVER_NAME
    if _SERVER_NAME is None:
        import socket
        _SERVER_NAME = socket.gethostname()
        # This seems to be flakey on some systems.
        #_SERVER_NAME = _system_to_string("uname -n")
    return _SERVER_NAME


def _get_conda_config():
    # TODO(*): Add your user and machine here.
    #
    # - For path
    # > which conda
    # /data/root/anaconda3/bin/conda
    # > find /data/root/anaconda3 -name "conda.sh"
    #
    # - For conda_env_path
    # > conda info
    # ...
    #        envs directories : /data/saggese/.conda/envs
    path = None
    conda_env_path = None
    if _get_user_name() == "saggese":
        if _get_server_name() in ("gpmac.local", "gpmac.lan"):
            path = "/Users/saggese/anaconda2/etc/profile.d/conda.sh"
            conda_env_path = "/Users/saggese/.conda/envs"
        elif _get_server_name().startswith("ip-"):
            path = "/data/root/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/data/saggese/.conda/envs"
    elif _get_user_name() == "paul":
        path = "/Users/paul/anaconda3/etc/profile.d/conda.sh"
        conda_env_path = "/Users/paul/.conda/envs"
    elif _get_user_name() == "jenkins":
        path = "/data/shared/anaconda2/etc/profile.d/conda.sh"
        conda_env_path = "/data/jenkins/.conda/envs"
    #
    if path is None:
        raise RuntimeError("username='%s' servername='%s' did not set 'path'. "
                           "Add your information to this file." %
                           (_get_user_name(), _get_server_name()))
    path = os.path.abspath(path)
    dbg.dassert_exists(path)
    #
    if conda_env_path is None:
        raise RuntimeError(
            "username='%s' servername='%s' did not set 'conda_env_path'. "
            "Add your information to this file." % (_get_user_name(),
                                                    _get_server_name()))
    conda_env_path = os.path.abspath(conda_env_path)
    # Not necessarily the conda_env_path exists.
    dbg.dassert_exists(os.path.dirname(conda_env_path))
    return path, conda_env_path


def get_conda_config_path():
    path, conda_env_path = _get_conda_config()
    _ = conda_env_path
    return path


def get_conda_env_path():
    path, conda_env_path = _get_conda_config()
    _ = path
    return conda_env_path


def _main():
    path = get_conda_config_path()
    print(path)


if __name__ == '__main__':
    _main()