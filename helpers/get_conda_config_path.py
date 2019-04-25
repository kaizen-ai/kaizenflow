#!/usr/bin/env python

import os

# TODO(gp): Not sure this is a good idea since it might create cyclic dependencies.
import helpers.dbg as dbg
import helpers.system_interaction as hsi


def _get_conda_config():
    # TODO(*): Add your user and machine here.
    path = None
    conda_env_path = None
    if hsi.USER_NAME == "saggese":
        #if hsi.SERVER_NAME == "ura":
        path = "/Users/saggese/anaconda2/etc/profile.d/conda.sh"
        conda_env_path = "/Users/saggese/.conda/envs"
    elif hsi.USER_NAME == "jenkins":
        path = "/data/shared/anaconda2/etc/profile.d/conda.sh"
        conda_env_path = "/data/jenkins/.conda/envs"
    #
    if path is None:
        raise RuntimeError("username='%s' servername='%s' did not set 'path'. "
                           "Add your information to this file." %
                           (hsi.USER_NAME, hsi.SERVER_NAME))
    path = os.path.abspath(path)
    dbg.dassert_exists(path)
    #
    if conda_env_path is None:
        raise RuntimeError(
            "username='%s' servername='%s' did not set 'conda_env_path'. "
            "Add your information to this file." % (hsi.USER_NAME,
                                                    hsi.SERVER_NAME))
    conda_env_path = os.path.abspath(conda_env_path)
    # Not necessarily the conda_env_path exists.
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
