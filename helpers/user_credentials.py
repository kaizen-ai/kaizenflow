import logging
import os

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# Servers.
DEV_SERVER = "104.248.187.204"
DB_SERVER = ""

# Server ports.
_MAPPING = {
    # "JENKINS": ("Continuous integration", DEV_SERVER, 8111),
    # "REVIEWBOARD": ("Code review", DEV_SERVER, 8080),
    # "NGINX": ("Publish notebook server", DEV_SERVER, 8181),
    # "NETDATA": ("System performance", DEV_SERVER, 19999),
}


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
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    #
    git_user_name = None
    git_user_email = None
    conda_sh_path = None
    conda_env_path = None
    key_path = None
    tunnel_info = None
    jupyter_port = None
    if user_name == "saggese":
        # GP.
        git_user_name = "saggese"
        git_user_email = "saggese@gmail.com"
        key_path = "~/.ssh/id_rsa"
        if server_name in ("gpmac.local", "gpmac.lan"):
            conda_sh_path = "/Users/saggese/anaconda2/etc/profile.d/conda.sh"
            conda_env_path = "/Users/saggese/.conda/envs"
            if git_repo_name == "ParticleDev/commodity_research":
                tunnel_info = [
                    ("Jupyter1", DEV_SERVER, 10001),
                    ("Mongodb", DEV_SERVER, 27017),
                ]
                jupyter_port = 10001
            elif git_repo_name == "alphamatic/lemonade":
                # TODO(gp): This should be factored out in the including
                #  superproject.
                jupyter_port = 9999
        elif server_name.startswith("ip-"):
            conda_sh_path = "/data/root/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/data/saggese/.conda/envs"
        elif server_name == "twitter-data":
            conda_sh_path = "/usr/sbin/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/saggese/.conda/envs"
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
    for var_name, val_name in [
        ("git_user_name", git_user_name),
        ("git_user_email", git_user_email),
        ("conda_sh_path", conda_sh_path),
        ("conda_env_path", conda_env_path),
        # We allow the rest of the variables (e.g., key_path, tunnel_info) to
        # be empty since in some configurations they can be undefined.
    ]:
        dbg.dassert_is_not(
            val_name,
            None,
            "Undefined '%s': add your credentials for user_name='%s' and "
            "server_name='%s' to '%s'",
            var_name,
            user_name,
            server_name,
            __file__,
        )
    conda_sh_path = os.path.abspath(conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
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
        "key_path": key_path,
        "tunnel_info": tunnel_info,
        "jupyter_port": jupyter_port,
    }
    return ret
