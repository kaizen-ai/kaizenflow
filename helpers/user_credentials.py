#!/usr/bin/env python
"""
Import as:

import helpers.user_credentials as usc

# Test that all the credentials are properly defined.
> helpers/user_credentials.py
"""

import argparse
import logging
import os
import pprint

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# TODO(gp): Add also P1_GDRIVE_PATH and AM_GDRIVE_PATH instead of using an env var.


def _get_p1_dev_server_ip():
    env_var_name = "P1_DEV_SERVER"
    if env_var_name not in os.environ:
        _LOG.error("Can't find '%s': re-run dev_scripts/setenv.sh", env_var_name)
        raise RuntimeError
    dev_server = os.environ[env_var_name]
    return dev_server


# pylint: disable=R0915
# [R0915(too-many-statements), ] Too many statements (51/50)
def get_credentials():
    """
    Report information about a user set-up as a function of:
        1) user name
        2) server name
        3) git repository name

    The mandatory information are:
    1) git_user_name
    2) git_user_email
    3) conda_sh_path: the path of the script bootstrapping conda
        - To find "conda_sh_path":
          > which conda
          /data/root/anaconda3/bin/conda
          > find /data/root/anaconda3 -name "conda.sh"
        - In one instruction:
          > CONDA_DIR=$(dirname $(which conda))"/.."; find $CONDA_DIR -name "conda.sh"
        - If there are multiple ones you want to pick the one under
          `profile.d`, e.g., `/anaconda3/etc/profile.d/conda.sh`
    4) conda_env_path: the path of the dir storing the conda environments
        - To find "conda_env_path"
          > conda info
          ...
                 envs directories : /data/saggese/.conda/envs

    The optional information are:
    5) ssh_key_path: the path of the ssh key to use
    6) tunnel_info: list of ports to forward
    7) jupyter_port: on which port to start a jupyter server on a specific server
        - It's a good idea for everybody to have a different port to avoid port
          collisions
    8) notebook_html_path: the path where to save html of notebooks
    9) notebook_backup_path: the path where to backup the source .ipynb code of
        notebooks
    """
    #
    user_name = si.get_user_name()
    server_name = si.get_server_name()
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    # Values to assign.
    git_user_name = None
    git_user_email = None
    conda_sh_path = None
    conda_env_path = None
    ssh_key_path = "~/.ssh/id_rsa"
    tunnel_info = []
    jupyter_port = None
    notebook_html_path = None
    notebook_backup_path = None
    #
    conda_env_path = "~/.conda/envs"
    conda_env_path = os.path.expanduser(conda_env_path)
    if server_name == "twitter-data":
        # P1 old server.
        conda_sh_path = "/usr/sbin/anaconda3/etc/profile.d/conda.sh"
    elif server_name == "ip-172-31-16-23":
        # P1 server.
        conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
    #
    if user_name == "saggese":
        # GP.
        git_user_name = "saggese"
        git_user_email = "saggese@gmail.com"
        if server_name in ("gpmac.local", "gpmac.lan"):
            # Laptop.
            conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/Users/saggese/.conda/envs"
            if git_repo_name == "ParticleDev/commodity_research":
                # TODO(gp): It is not obvious how to use this part, we have to
                # introduce this way to the team. it seems that nobody uses
                # this way tho forward ports for jupyter or any else service.
                service = ("Jupyter1", _get_p1_dev_server_ip(), 10003, 10003)
                tunnel_info = [service]
                jupyter_port = 10001
            elif git_repo_name == "alphamatic/lemonade":
                # TODO(gp): This should be factored out in the including
                #  superproject.
                jupyter_port = 9999
                notebook_html_path = "/Users/saggese/src/notebooks"
                notebook_backup_path = "/Users/saggese/src/notebooks/backup"
        elif server_name == "twitter-data":
            # P1 old server.
            if git_repo_name == "ParticleDev/commodity_research":
                jupyter_port = 10002
        elif server_name == "ip-172-31-16-23":
            # P1 server.
            if git_repo_name == "ParticleDev/commodity_research":
                jupyter_port = 10003
        elif server_name.startswith("ip-"):
            # AM server.
            conda_sh_path = "/data/root/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/data/saggese/.conda/envs"
    elif user_name == "paul":
        # Paul.
        git_user_name = "paul"
        git_user_email = "smith.paul.anthony@gmail.com"
        if server_name in ("Pauls-MacBook-Pro.local", "Pauls-MBP"):
            conda_sh_path = "/Users/paul/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/Users/paul/.conda/envs"
    elif user_name == "gad":
        # Sergey.
        if server_name == "ip-172-31-16-23":
            git_user_name = "gad26032"
            git_user_email = "malanin@particle.one"
            conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/gad/.conda/envs"
            jupyter_port = 9111
        if server_name == "particle-laptop":
            git_user_name = "gad26032"
            git_user_email = "malanin@particle.one"
            conda_sh_path = "/home/gad/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/gad/.conda/envs"
            jupyter_port = 9111
        service = ("Jupyter", _get_p1_dev_server_ip(), jupyter_port, jupyter_port)
        tunnel_info.append(service)
    elif user_name == "julia":
        # Julia.
        git_user_name = "Julia"
        git_user_email = "julia@particle.one"
        jupyter_port = 9997
        if server_name == "vostro":
            # Laptop.
            conda_sh_path = "/home/julia/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/julia/.conda/envs"
    elif user_name == "sonniki":
        # Sonya.
        git_user_name = "sonniki"
        git_user_email = "sonya@particle.one"
        conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
        conda_env_path = "/home/sonniki/.conda/envs"
    elif user_name == "liza":
        # Liza.
        git_user_name = "lizvladi"
        git_user_email = "elizaveta@particle.one"
        jupyter_port = 9992
        if server_name == "liza-particle-laptop":
            # Laptop.
            conda_sh_path = "/home/liza/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/liza/anaconda3/envs"
    elif user_name == "stas":
        # Stas.
        git_user_name = "tsallagov"
        git_user_email = "stanislav@particle.one"
        jupyter_port = 9900
        if server_name == "stas-Vostro-5471":
            # Laptop.
            conda_sh_path = "/home/stas/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/stas/anaconda3/envs"
    elif user_name == "jenkins":
        # Jenkins.
        # Jenkins should not commit so it doesn't neet Git credentials.
        git_user_name = ""
        git_user_email = ""
        conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
        conda_env_path = "/var/lib/jenkins/.conda/envs"
    # Check.
    for var_name, val_name in [
        ("git_user_name", git_user_name),
        ("git_user_email", git_user_email),
        ("conda_sh_path", conda_sh_path),
        ("conda_env_path", conda_env_path),
        # We allow the rest of the variables (e.g., ssh_key_path, tunnel_info) to
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
    for service in tunnel_info:
        # TODO(gp): We should call in ssh_tunnels.py to keep this encapsulated.
        dbg.dassert_eq(len(service), 4)
        service_name, server, local_port, remote_port = service
        _ = service_name, server, local_port, remote_port
    ret = {
        "git_user_name": git_user_name,
        "git_user_email": git_user_email,
        "conda_sh_path": conda_sh_path,
        "conda_env_path": conda_env_path,
        "ssh_key_path": ssh_key_path,
        "tunnel_info": tunnel_info,
        "jupyter_port": jupyter_port,
        "notebook_html_path": notebook_html_path,
        "notebook_backup_path": notebook_backup_path,
    }
    return ret


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--user", action="store", default=None, help="Impersonate a user"
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    if args.user:
        si.set_user_name(args.user)
    usc = get_credentials()
    pprint.pprint(usc)


if __name__ == "__main__":
    _main(_parse())
