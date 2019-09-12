import logging
import os

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def get_credentials():
    """
    - To find "conda_sh_path":
      > which conda
      /data/root/anaconda3/bin/conda
      > find /data/root/anaconda3 -name "conda.sh"

    - To find "conda_env_path"
      > conda info
      ...
             envs directories : /data/saggese/.conda/envs
    """
    user_name = si.get_user_name()
    server_name = si.get_server_name()
    git_user_name = None
    git_user_email = None
    conda_sh_path = None
    conda_env_path = None
    if user_name in ("saggese", "gp"):
        # GP.
        git_user_name = "saggese"
        git_user_email = "saggese@gmail.com"
        if server_name in ("gpmac.local", "gpmac.lan"):
            conda_sh_path = "/Users/saggese/anaconda2/etc/profile.d/conda.sh"
            conda_env_path = "/Users/saggese/.conda/envs"
        elif server_name.startswith("ip-"):
            conda_sh_path = "/data/root/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/data/saggese/.conda/envs"
        elif server_name == "twitter-data":
            conda_sh_path = "/usr/sbin/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/gp/.conda/envs"
    elif user_name == "paul":
        # Paul.
        git_user_name = "paul"
        git_user_email = "smith.paul.anthony@gmail.com"
        conda_sh_path = "/Users/paul/anaconda3/etc/profile.d/conda.sh"
        conda_env_path = "/Users/paul/.conda/envs"
    elif user_name == "julia":
        # Julia.
        git_user_name = "Julia"
        git_user_email = "julia@particle.one"
    elif user_name == "jenkins":
        # Jenkins.
        # Jenkins should not commit so it doesn't neet Git credentials.
        git_user_name = ""
        git_user_email = ""
        conda_sh_path = "/data/shared/anaconda2/etc/profile.d/conda.sh"
        conda_env_path = "/data/jenkins/.conda/envs"
    # Check.
    dbg.dassert_is_not(
        git_user_name,
        None,
        "Add your credentials for user_name='%s' and server_name='%s' to '%s'",
        user_name,
        server_name,
        __file__,
    )
    dbg.dassert_is_not(
        git_user_email,
        None,
        "Add your credentials for user_name='%s' and server_name='%s' to '%s'",
        user_name,
        server_name,
        __file__,
    )
    dbg.dassert_is_not(
        conda_sh_path,
        None,
        "Add your credentials for user_name='%s' and server_name='%s' to '%s'",
        user_name,
        server_name,
        __file__,
    )
    conda_sh_path = os.path.abspath(conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
    dbg.dassert_is_not(
        conda_env_path,
        None,
        "Add your credentials for user_name='%s' and server_name='%s' to '%s'",
        user_name,
        server_name,
        __file__,
    )
    conda_env_path = os.path.abspath(conda_env_path)
    # Not necessarily the conda_env_path exists.
    if not os.path.exists(conda_env_path):
        _LOG.warning("The dir '%s' doesn't exist: creating it", conda_env_path)
        io_.create_dir(conda_env_path, incremental=True)
    dbg.dassert_exists(os.path.dirname(conda_env_path))
    #
    ret = {
        "git_user_name": git_user_name,
        "git_user_email": git_user_email,
        "conda_sh_path": conda_sh_path,
        "conda_env_path": conda_env_path,
    }
    return ret
