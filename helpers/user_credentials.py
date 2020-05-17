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
from typing import Any, Dict

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# TODO(gp): Add also P1_GDRIVE_PATH and AM_GDRIVE_PATH instead of using an env var.


def get_p1_dev_server_ip() -> str:
    """
    Get the dev server name from the user environment.
    """
    env_var_name = "P1_DEV_SERVER"
    if env_var_name not in os.environ:
        _LOG.error("Can't find '%s': re-run dev_scripts/setenv.sh?", env_var_name)
        raise RuntimeError
    dev_server = os.environ[env_var_name]
    return dev_server


# pylint: disable=too-many-statements
def get_credentials() -> Dict[str, Any]:
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
    6) tunnel_info: list of "personal" ports to forward
        - This is an advanced behavior that allows to specify in your user
          config a set of ports to forward from one computer (typically your
          laptop) to a set of services that are specific of your set-up (e.g.,
          started through `run_jupyter_server.py`)
        - E.g.,
          ```python
            if server_name in ("gpmac.local", "gpmac.lan"):
                if git_repo_name == "ParticleDev/commodity_research":
                    service = ("Jupyter1", get_p1_dev_server_ip(), 10003, 10003)
          ```
          when GP runs `ssh_tunnels.py` from his laptop in a
          `ParticleDev/commodity_research` client, a tunnel is open to the dev
          server where `run_jupyter_server.py` will have started a notebook server
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
    _LOG.debug("user_name='%s'", user_name)
    _LOG.debug("server_name='%s'", server_name)
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    # Values to assign.
    git_user_name = ""
    git_user_email = ""
    conda_sh_path = ""
    ssh_key_path = "~/.ssh/id_rsa"
    tunnel_info = []
    jupyter_port = -1
    notebook_html_path = ""
    notebook_backup_path = ""
    #
    conda_env_path = "~/.conda/envs"
    conda_env_path = os.path.expanduser(conda_env_path)
    if server_name == "twitter-data":
        # P1 old server.
        conda_sh_path = "/usr/sbin/anaconda3/etc/profile.d/conda.sh"
    elif server_name in (
        # P1 dev server.
        "ip-172-31-16-23",
        # P1 Jenkins server.
        "ip-172-31-12-239",
    ):
        conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
    if user_name == "saggese":
        # GP.
        git_user_name = "saggese"
        git_user_email = "saggese@gmail.com"
        if server_name in ("gpmac.local", "gpmac.lan", "giacintos-mbp.lan"):
            # Laptop.
            conda_sh_path = "/Users/saggese/opt/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/Users/saggese/.conda/envs"
            if git_repo_name == "ParticleDev/commodity_research":
                # Forward port 10003 to the notebook server that is started by
                # `run_jupyter_server.py` when executed on the dev server.
                # service = ("Jupyter1", get_p1_dev_server_ip(), 10003, 10003)
                # tunnel_info.append(service)
                # jupyter_port = 10001
                pass
        elif server_name == "twitter-data":
            # P1 old server.
            if git_repo_name == "ParticleDev/commodity_research":
                jupyter_port = 10002
        elif server_name == "ip-172-31-16-23":
            # P1 server.
            if git_repo_name == "ParticleDev/commodity_research":
                jupyter_port = 10003
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
            conda_env_path = "~/.conda/envs"
            jupyter_port = 9111
        elif server_name == "particle-laptop":
            git_user_name = "gad26032"
            git_user_email = "malanin@particle.one"
            conda_sh_path = "~/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "~/.conda/envs"
            jupyter_port = 9111
        # service = ("Jupyter", get_p1_dev_server_ip(), jupyter_port, jupyter_port)
        # tunnel_info.append(service)
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
        conda_env_path = os.path.expanduser("~/.conda/envs")
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
    elif user_name == "daniil":
        # Daniil.
        git_user_name = "mongolianjesus"
        git_user_email = "daniil@particle.one"
        jupyter_port = 9699
        if server_name == "daniil":
            # Laptop.
            conda_sh_path = "/home/daniil/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/daniil/anaconda3/envs"
        elif server_name == "Daniils-MacBook-Air.local":
            # Home laptop.
            conda_sh_path = "/Users/danya/opt/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/Users/danya/opt/anaconda3/envs" 
    elif user_name == "dan":
        # Dan.
        git_user_name = "DanilYachmenev"
        git_user_email = "dan@particle.one"
        jupyter_port = 9901
        if server_name == "dan-particle-laptop":
            # Laptop.
            conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/home/dan/anaconda3/envs"
    elif user_name == "greg":
        # Gregory.
        git_user_name = "greg-ptcl"
        git_user_email = "greg@particle.one"
        jupyter_port = 9666
        if server_name == "computer":
            # Laptop.
            conda_sh_path = "/home/greg/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = os.path.expanduser("/home/greg/anaconda3/envs")
    elif user_name == "max_particle":
        # MaxParticle.
        git_user_name = "MaxParticle"
        git_user_email = "max@particle.one"
    elif user_name == "asya":
        # Asya.
        git_user_name = "ultrasya"
        git_user_email = "asya@particle.one"
        jupyter_port = 9698
        if server_name == "MacBook-Pro-Asa.local":
            # Home laptop.
            conda_sh_path = "/Users/asya/opt/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "/Users/asya/.conda/envs"
    elif user_name == "grisha":
        # Grisha Pomazkin.
        git_user_name = "PomazkinG"
        git_user_email = "pomazkinG@particle.one"
        jupyter_port = 9631
        if server_name == "particle-grisha":
            # Home laptop.
            conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "~/.conda/envs"
    elif user_name == "jenkins":
        # Jenkins.
        # Jenkins should not commit so it doesn't neet Git credentials.
        git_user_name = ""
        git_user_email = ""
        conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
        conda_env_path = "/var/lib/jenkins/.conda/envs"
    # We use this for #1522, #1831.
    elif server_name == "docker-instance" or user_name == 'root':
        # Docker user.
        git_user_name = "infraparticleone"
        git_user_email = "infra@particle.one"
        conda_sh_path = "/opt/conda/etc/profile.d/conda.sh"
        conda_env_path = "~/.conda/envs/"
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
    conda_sh_path = os.path.expanduser(conda_sh_path)
    conda_sh_path = os.path.abspath(conda_sh_path)
    dbg.dassert_exists(conda_sh_path)
    #
    conda_env_path = os.path.abspath(os.path.expanduser(conda_env_path))
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
    _LOG.debug("Credentials: %s", ret)
    return ret


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--user", action="store", default=None, help="Impersonate a user"
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    if args.user:
        si.set_user_name(args.user)
    usc = get_credentials()
    pprint.pprint(usc)


if __name__ == "__main__":
    _main(_parse())
