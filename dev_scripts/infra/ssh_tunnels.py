#!/usr/bin/env python
"""
# Start all tunnels
> ssh_tunnels.py start

# Stop all service tunnels
> ssh_tunnels.py stop

# Report the status of each service tunnel
> ssh_tunnels.py check

# Kill all the ssh tunnels on the machine, for a known service or not.
> ssh_tunnels.py kill
"""

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)


# ##############################################################################


def _get_tunnel_info():
    credentials = usc.get_credentials()
    #
    tunnel_info = credentials["tunnel_info"]
    dbg.dassert_is_not(tunnel_info, None)
    #
    ssh_key_path = credentials["ssh_key_path"]
    dbg.dassert_is_not(ssh_key_path, None)
    return tunnel_info, ssh_key_path


def _get_ssh_tunnel_process(port):
    """
    Return the pids of the processes attached to a given port.
    """

    def _keep_line(port, line):
        keep = ("ssh -i" in line) and (("localhost:%d" % port) in line)
        return keep

    _LOG.debug("port=%s", port)
    keep_line = lambda line: _keep_line(port, line)
    pids, txt = si.get_process_pids(keep_line)
    _LOG.debug("pids=%s", pids)
    if len(pids) > 1:
        _LOG.warning("Expected a single process, instead got:\n%s", txt)
    return pids, txt


# ##############################################################################


def _create_tunnel(server_name, port, user_name, ssh_key_path):
    """
    Create tunnel from localhost to 'server' for the given `port` and
    `user_name`.
    """
    ssh_key_path = os.path.expanduser(ssh_key_path)
    _LOG.debug("ssh_key_path=%s", ssh_key_path)
    dbg.dassert_exists(ssh_key_path)
    #
    cmd = (
        "ssh -i {ssh_key_path} -f -nNT -L {port}:localhost:{port}"
        + " {user_name}@{server}"
    )
    cmd = cmd.format(
        user_name=user_name,
        ssh_key_path=ssh_key_path,
        port=port,
        server=server_name,
    )
    si.system(cmd, blocking=False)
    # Check that the tunnel is up and running.
    pids = _get_ssh_tunnel_process(port)
    dbg.dassert_lte(1, len(pids))


def _kill_ssh_tunnel_process(port):
    """
    Kill all the processes attached to a given port.
    """
    get_pids = lambda: _get_ssh_tunnel_process(port)
    si.kill_process(get_pids)


# ##############################################################################


def _start_tunnels(user_name):
    """
    Start all the tunnels for the given user.
    """
    _LOG.debug("user_name=%s", user_name)
    # Get tunnel info.
    tunnel_info, ssh_key_path = _get_tunnel_info()
    _LOG.info("tunnel_info=%s", tunnel_info)
    for service_name, server, port in tunnel_info:
        pids, _ = _get_ssh_tunnel_process(port)
        if not pids:
            _LOG.info(
                "Starting tunnel for service '%s' server=%s port=%s",
                service_name,
                server,
                port,
            )
            _create_tunnel(server, port, user_name, ssh_key_path)
        else:
            _LOG.warning(
                "Tunnel for service '%s' on port %s already exist: skipping",
                service_name,
                port,
            )


def _stop_tunnels():
    """
    Stop all the tunnels for the given user.
    """
    # Get the tunnel info.
    tunnel_info, _ = _get_tunnel_info()
    _LOG.info("tunnel_info=%s", tunnel_info)
    #
    for service_name, server, port in tunnel_info:
        _LOG.info(
            "Stopping tunnel for service '%s' server=%s port=%s",
            service_name,
            server,
            port,
        )
        _kill_ssh_tunnel_process(port)


def _check_tunnels():
    """
    Check the status of the tunnels for the given user.
    """
    # Get the tunnel info.
    tunnel_info, _ = _get_tunnel_info()
    _LOG.info("tunnel_info=%s", tunnel_info)
    #
    for service_name, server, port in tunnel_info:
        pids, _ = _get_ssh_tunnel_process(port)
        if pids:
            msg = "exists with pid=%s" % pids
        else:
            msg = "doesn't exist"
        _LOG.info(
            "service='%s' server=%s port=%s %s", service_name, server, port, msg
        )


def _kill_all_tunnel_processes():
    # cmd = "ps ax | grep 'ssh -i' | grep localhost: | grep -v grep"
    def _keep_line(line):
        keep = ("ssh -i" in line) and ("localhost:" in line)
        return keep

    get_pids = lambda: si.get_process_pids(_keep_line)
    si.kill_process(get_pids)


# ##############################################################################


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    _help = """
- start: start a notebook if no notebook server is running at the requested port
- check: print the notebook servers running
- kill: kill a notebook server squatting the requested port
- force_start: kill squatting notebook servers and start a new one
"""
    parser.add_argument(
        "positional",
        nargs=1,
        choices=["start", "stop", "check", "kill"],
        help=_help,
    )
    parser.add_argument("--user", type=str, action="store")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    if args.user:
        user_name = args.user
    else:
        user_name = si.get_user_name()
    #
    action = args.positional[0]
    _LOG.debug("action=%s", action)
    if action == "start":
        _start_tunnels(user_name)
    elif action == "stop":
        _stop_tunnels()
    elif action == "check":
        _check_tunnels()
    elif action == "kill":
        _kill_all_tunnel_processes()
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main()
