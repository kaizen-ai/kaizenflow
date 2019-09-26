import json
import logging

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)


def conda_system(cmd, *args, **kwargs):
    """
    When running a conda command we need to execute a script to configure conda.
    This script is typically executed in .bashrc but here we create a new bash
    shell every time to execute a command, so we need to re-initialize the shell
    before any conda command.

    :param cmd:
    :param args:
    :param kwargs:
    :return:
    """
    # TODO(gp): Pass conda_env_name as done in get_conda_list()
    path = usc.get_credentials()["conda_sh_path"]
    dbg.dassert_exists(path)
    cmd = "source %s && %s" % (path, cmd)
    return si.system(cmd, *args, **kwargs)


def conda_system_to_string(cmd, *args, **kwargs):
    path = usc.get_credentials()["conda_sh_path"]
    dbg.dassert_exists(path)
    cmd = "source %s && %s" % (path, cmd)
    return si.system_to_string(cmd, *args, **kwargs)


def get_conda_envs_dirs():
    """
    :return: list of the env dirs from conda
    """
    _, ret = conda_system_to_string(r"conda config --show envs_dirs --json")
    _LOG.debug("ret=%s", ret)
    envs = json.loads(ret)
    dbg.dassert_in("envs_dirs", envs)
    envs = envs["envs_dirs"]
    dbg.dassert_isinstance(envs, list)
    return envs


def set_conda_env_root(conda_env_path):
    """
    Set conda env dirs so that it matches what specified in

    > conda config --show envs_dirs --json
    {
      "envs_dirs": [
        "/Users/gp/.conda/envs",
      ]
    }

    > conda config --prepend envs_dirs /data/gp_wd/anaconda2/envs2
    """
    envs = get_conda_envs_dirs()
    #
    if not envs or envs[0] != conda_env_path:
        _LOG.warning(
            "%s is not the first env dir in %s", conda_env_path, str(envs)
        )
        # Reset the list of conda envs.
        _LOG.debug("Resetting envs_dir %s", str(envs))
        for env in envs:
            _LOG.debug("Deleting %s", env)
            cmd = "conda config --remove envs_dirs %s" % env
            # We don't abort because of a bug in conda not deleting the key
            # when asked for.
            # CondaKeyError: 'envs_dirs': u'/data/shared/anaconda2/envs' is not
            #   in the u'envs_dirs' key of the config file
            conda_system(cmd, abort_on_error=False)
        envs = get_conda_envs_dirs()
        _LOG.debug("Current envs: %s", str(envs))
        # Add the conda env.
        cmd = r"conda config --prepend envs_dirs %s" % conda_env_path
        conda_system(cmd)
        # Check.
        envs = get_conda_envs_dirs()
        dbg.dassert(
            envs or envs[0] != conda_env_path,
            msg="%s is not first env dir in %s" % (conda_env_path, envs),
        )
    else:
        _LOG.debug(
            "Nothing to do, since %s is already in %s", conda_env_path, envs
        )


def get_conda_info_envs():
    """
    :return: (env_dict, active_env)
        - env_dict: map 'conda env name -> conda env path'
        - active_env: name of the active conda env
    """
    # > conda info --envs
    # # conda environments:
    # #
    # aws                      /Users/gp/.conda/envs/aws
    # bbg                      /Users/gp/.conda/envs/bbg
    # deeplearning             /Users/gp/.conda/envs/deeplearning
    # jupyter                  /Users/gp/.conda/envs/jupyter
    # test_conda               /Users/gp/.conda/envs/test_conda
    # TODO(gp): Use --json but we need to parse the json without any module.
    ret = conda_system_to_string(r"conda info --envs")[1]
    _LOG.debug("Parsing conda info\n%s", ret)
    ret = ret.split("\n")
    env_dict = {}
    active_env = None
    for line in ret:
        line = line.rstrip().lstrip()
        if line == "":
            continue
        if line.startswith("#"):
            continue
        vals = line.split()
        if len(vals) == 2:
            env_name, env_path = vals
            env_dict[env_name] = env_path
        elif len(vals) == 3:
            env_name, star, env_path = vals
            dbg.dassert_eq(star, "*")
            env_dict[env_name] = env_path
        else:
            _LOG.debug("Can't parse line='%s'", line)
    return env_dict, active_env


def get_conda_list(conda_env_name):
    """
    :return: env_dict mapping package name to their info
        - env_dict: map 'conda env name -> conda env path'
        - active_env: name of the active conda env
    """
    # > conda list
    # # packages in environment at /Users/gp/.conda/envs/:
    # #
    # # Name                    Version                   Build  Channel
    # absl-py                   0.5.0                      py_0    conda-forge
    # agate                     1.6.0                      py_3    conda-forge
    # agate-dbf                 0.2.0                    py27_0    conda-forge
    # agate-excel               0.2.2                      py_0    conda-forge
    # TODO(gp): Use --json but we need to parse the json without any module.
    cmd = r"(conda activate %s 2>&1) >/dev/null && conda list" % conda_env_name
    ret = conda_system_to_string(cmd)[1]
    ret = ret.split("\n")
    env_dict = {}
    labels = {1: "version", 2: "build", 3: "channel"}
    for l in ret:
        l = l.rstrip().lstrip()
        _LOG.debug("line='%s'", l)
        if l == "":
            continue
        if l.startswith("#"):
            continue
        vals = l.split()
        env_dict[vals[0]] = {labels[k]: vals[k] for k in range(1, len(vals[:4]))}
    return env_dict


_CONDA_PATH = None


def get_conda_path():
    global _CONDA_PATH
    if not _CONDA_PATH:
        rc, txt = _CONDA_PATH = conda_system_to_string("which conda", abort_on_error=False)
        if rc == 0:
            _CONDA_PATH = txt
        else:
            _CONDA_PATH = "n/a"
    return _CONDA_PATH
