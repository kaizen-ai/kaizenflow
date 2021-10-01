"""
Import as:

import helpers.old.tunnels as holtun
"""

import logging
import os
from typing import Dict, List, Tuple, cast

import helpers.dbg as hdbg
import helpers.old.user_credentials as holuscre
import helpers.printing as hprintin
import helpers.system_interaction as hsyint

_LOG = logging.getLogger(__name__)

# #############################################################################


def get_tunnel_info() -> Tuple[list, str]:
    credentials = holuscre.get_credentials()
    #
    tunnel_info = credentials["tunnel_info"]
    hdbg.dassert_is_not(tunnel_info, None)
    # Add tunnels for standard services.
    services = _get_services_info()
    tunnel_info.extend(services)
    #
    ssh_key_path = credentials["ssh_key_path"]
    hdbg.dassert_is_not(ssh_key_path, None)
    # TODO(gp): Add check to make sure that the source ports are all different.
    return tunnel_info, ssh_key_path


def tunnel_info_to_string(tunnel_info: list) -> str:
    ret = "\n".join(map(str, tunnel_info))
    ret = hprintin.indent(ret)
    return ret


def parse_service(service: str) -> Dict[str, str]:
    hdbg.dassert_eq(len(service), 4, "service=%s", service)
    service_name, server, local_port, remote_port = service
    return {
        "service_name": service_name,
        "server": server,
        "local_port": local_port,
        "remote_port": remote_port,
    }


def find_service(service_name: str, tunnel_info: list) -> str:
    ret = None
    for service in tunnel_info:
        if service_name == parse_service(service)["service_name"]:
            hdbg.dassert_is(ret, None)
            ret = service
    hdbg.dassert_is_not(ret, None)
    return ret


def get_server_ip(service_name: str):  # pylint: disable=unused-argument
    tunnel_info, _ = get_tunnel_info()
    _LOG.debug("tunnels=\n%s", tunnel_info_to_string(tunnel_info))
    service = find_service("Doc server", tunnel_info)
    server = parse_service(service)["server"]
    return server


def _get_services_info() -> list:
    # Server ports.
    services = [
        # service name, server public IP, local port, remote port.
        ("MongoDb", hsyint.get_env_var("OLD_DEV_SERVER"), 27017, 27017),
        ("Jenkins", hsyint.get_env_var("JENKINS_SERVER"), 8080, 8080),
        # ("Reviewboard", hsyint.get_env_var("REVIEWBOARD_SERVER"), 8000, 8000),
        # ("Doc server", hsyint.get_env_var("REVIEWBOARD_SERVER"), 8001, 80),
        # Netdata to Jenkins and Dev server.
        # ("Dev system performance", DEV_SERVER, 19999),
        # ("Jenkins system performance", DEV_SERVER, 19999),
    ]
    return services


def _get_tunnel_info() -> Tuple[Any, str]:
    credentials = holuscre.get_credentials()
    #
    tunnel_info = credentials["tunnel_info"]
    hdbg.dassert_is_not(tunnel_info, None)
    # Add tunnels for standard services.
    services = _get_services_info()
    tunnel_info.extend(services)
    #
    ssh_key_path = credentials["ssh_key_path"]
    hdbg.dassert_is_not(ssh_key_path, None)
    # TODO(gp): Add check to make sure that the source ports are all different.
    return tunnel_info, ssh_key_path


def _tunnel_info_to_string(tunnel_info: list) -> str:
    ret = "\n".join(map(str, tunnel_info))
    ret = hprintin.indent(ret)
    cast(str, ret)
    return ret


def _service_to_string(service: Tuple[str, str, str, str]) -> str:
    service_name, server, local_port, remote_port = service
    ret = (
        f"tunnel for service '{service_name}'"
        + f" server='{server}'"
        + f" port='{local_port}->{remote_port}'"
    )
    return ret


# #############################################################################


def _get_ssh_tunnel_process(
    local_port: int, remote_port: int, fuzzy_match: bool
) -> Tuple[List[int], str]:
    """
    Return the pids of the processes attached to a given port.
    """

    def _keep_line(line: str) -> bool:
        keep = "ssh -i" in line
        if keep:
            if fuzzy_match:
                keep = (" %d:localhost " % local_port in line) or (
                    " localhost:%d " % remote_port in line
                )
            else:
                keep = " %d:localhost:%d " % (local_port, remote_port) in line
        return keep

    _LOG.debug("local_port=%d -> remote_port=%d", local_port, remote_port)
    pids, txt = hsyint.get_process_pids(_keep_line)
    _LOG.debug("pids=%s", pids)
    _LOG.debug("txt=\n%s", txt)
    return pids, txt


def _create_tunnel(
    server_name: str,
    local_port: int,
    remote_port: int,
    user_name: str,
    ssh_key_path: str,
) -> None:
    """
    Create tunnel from localhost to 'server' for the ports `local_port ->
    remote_port` and `user_name`.
    """
    ssh_key_path = os.path.expanduser(ssh_key_path)
    _LOG.debug("ssh_key_path=%s", ssh_key_path)
    hdbg.dassert_exists(ssh_key_path)
    #
    cmd = (
        "ssh -i {ssh_key_path} -f -nNT -L {local_port}:localhost:{remote_port}"
        + " {user_name}@{server}"
    )
    cmd = cmd.format(
        user_name=user_name,
        ssh_key_path=ssh_key_path,
        local_port=local_port,
        remote_port=remote_port,
        server=server_name,
    )
    hsyint.system(cmd, blocking=False)
    # Check that the tunnel is up and running.
    pids = _get_ssh_tunnel_process(local_port, remote_port, fuzzy_match=True)
    hdbg.dassert_lte(1, len(pids))


def _kill_ssh_tunnel_process(local_port: int, remote_port: int) -> None:
    """
    Kill all the processes attached to either local or remote port.
    """
    get_pids = lambda: _get_ssh_tunnel_process(
        local_port, remote_port, fuzzy_match=True
    )
    hsyint.kill_process(get_pids)


# #############################################################################


def start_tunnels(user_name: str) -> None:
    """
    Start all the tunnels for the given user.
    """
    _LOG.debug("user_name=%s", user_name)
    # Get tunnel info.
    tunnel_info, ssh_key_path = _get_tunnel_info()
    _LOG.info("\n%s", _tunnel_info_to_string(tunnel_info))
    #
    for service in tunnel_info:
        _, server, local_port, remote_port = service
        pids, _ = _get_ssh_tunnel_process(
            local_port, remote_port, fuzzy_match=False
        )
        if not pids:
            _LOG.info("Starting %s", _service_to_string(service))
            _create_tunnel(
                server, local_port, remote_port, user_name, ssh_key_path
            )
        else:
            _LOG.warning(
                "%s already exists: skipping", _service_to_string(service)
            )


def stop_tunnels() -> None:
    """
    Stop all the tunnels for the given user.
    """
    # Get the tunnel info.
    tunnel_info, _ = _get_tunnel_info()
    _LOG.info("\n%s", _tunnel_info_to_string(tunnel_info))
    #
    for service in tunnel_info:
        _, _, local_port, remote_port = service
        _LOG.info("Stopping %s", _service_to_string(service))
        _kill_ssh_tunnel_process(local_port, remote_port)


def check_tunnels() -> None:
    """
    Check the status of the tunnels for the given user.
    """
    # Get the tunnel info.
    tunnel_info, _ = _get_tunnel_info()
    _LOG.info("\n%s", _tunnel_info_to_string(tunnel_info))
    #
    for service in tunnel_info:
        _, _, local_port, remote_port = service
        pids, _ = _get_ssh_tunnel_process(
            local_port, remote_port, fuzzy_match=False
        )
        if pids:
            msg = "exists with pid=%s" % pids
        else:
            msg = "doesn't exist"
        _LOG.info("%s -> %s", _service_to_string(service), msg)


def kill_all_tunnel_processes() -> None:
    """
    Kill all the processes that have `ssh -i ...:localhost:...".
    """
    # cmd = "ps ax | grep 'ssh -i' | grep localhost: | grep -v grep"
    def _keep_line(line: str) -> bool:
        keep = ("ssh -i" in line) and (":localhost:" in line)
        return keep

    get_pids = lambda: hsyint.get_process_pids(_keep_line)
    hsyint.kill_process(get_pids)
