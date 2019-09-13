#!/usr/bin/env python
"""
# Start all tunnels
> ssh_tunnels.py --action start

# Stop all service tunnels
> ssh_tunnels.py --action stop

# Report the status of each service tunnel
> ssh_tunnels.py --action check

# Kill all the ssh tunnels on the machine, for a known service or not.
> ssh_tunnels.py --action killall
"""

import argparse
import logging
import os
import signal

import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# Servers.
DEV_SERVER = "104.248.187.204"
DB_SERVER = ""

# Server ports.
MAPPING = {
    # "JENKINS": ("Continuous integration", DEV_SERVER, 8111),
    # "REVIEWBOARD": ("Code review", DEV_SERVER, 8080),
    # "NGINX": ("Publish notebook server", DEV_SERVER, 8181),
    # "NETDATA": ("System performance", DEV_SERVER, 19999),
}

DEFAULT_PORTS = sorted(MAPPING.keys())

# Users


# ##############################################################################


def _get_tunnel_info(user_name):
    key_path = "~/.ssh/id_rsa"
    server_name = si.get_server_name()
    git_repo_name = git.get_repo_symbolic_name(super_module=True)
    info = None
    if user_name in ("gp", "saggese"):
        if git_repo_name == "ParticleDev/commodity_research":
            if server_name.startswith("gpmac."):
                info = [
                    ("Jupyter1", DEV_SERVER, 10001),
                    ("Mongodb", DEV_SERVER, 27017),
                ]
    dbg.dassert_is_not(info, None)
    return info, key_path


# ##############################################################################


def _create_tunnel(server_name, port, user_name, key_path):
    """
    Create tunnel from localhost to 'server' for the given `port` and
    `user_name`.
    """
    key_path = os.path.expanduser(key_path)
    _LOG.debug("key_path=%s", key_path)
    dbg.dassert_exists(key_path)
    #
    cmd = (
        "ssh -i {key_path} -f -nNT -L {port}:localhost:{port}"
        + "{user_name}@{server}"
    )
    cmd = cmd.format(
        user_name=user_name, key_path=key_path, port=port, server=server_name
    )
    si.system(cmd, blocking=False)


def _parse_ps_output(cmd):
    rc, txt = si.system_to_string(cmd, abort_on_error=False)
    _LOG.debug("txt=\n%s", txt)
    pids = []
    if rc == 0:
        for line in txt.split("\n"):
            _LOG.debug("line=%s", line)
            # pylint: disable=C0301
            # > ps ax | grep 'ssh -i' | grep localhost
            # 19417   ??  Ss     0:00.39 ssh -i /Users/gp/.ssh/id_rsa -f -nNT \
            #           -L 19999:localhost:19999 gp@54.172.40.4
            fields = line.split()
            try:
                pid = int(fields[0])
            except ValueError as e:
                _LOG.error("Cant' parse '%s' from '%s'", fields, line)
                raise e
            _LOG.debug("pid=%s", pid)
            pids.append(pid)
    return pids, txt


def _get_ssh_tunnel_process(port):
    """
    Return the pids of the processes attached to a given port.
    """
    _LOG.debug("port=%s", port)
    cmd = "ps ax | grep 'ssh -i' | grep localhost:{port} | grep -v grep".format(
        port=port
    )
    pids, txt = _parse_ps_output(cmd)
    if len(pids) > 1:
        _LOG.debug("Expect a single process, instead got:\n%s", txt)
    _LOG.debug("pids=%s", pids)
    return pids


def _kill_ssh_tunnel_process(port):
    """
    Kill all the processes attached to a given port.
    """
    pids = _get_ssh_tunnel_process(port)
    for pid in pids:
        os.kill(pid, signal.SIGKILL)


# ##############################################################################


def _start_tunnels(user_name):
    """
    Start all the tunnels for the given user.
    """
    _LOG.debug("user_name=%s", user_name)
    # Get tunnel info.
    info, key_path = _get_tunnel_info(user_name)
    _LOG.info("info=%s", info)
    for service_name, server, port in info:
        pids = _get_ssh_tunnel_process(port)
        if not pids:
            _LOG.info(
                "Starting tunnel for service '%s' server=%s port=%s",
                service_name,
                server,
                port,
            )
            _create_tunnel(server, port, user_name, key_path)
        else:
            _LOG.warning(
                "Tunnel for service '%s' on port %s already exist: skipping",
                service_name,
                port,
            )


def _stop_tunnels(user_name):
    """
    Stop all the tunnels for the given user.
    """
    _LOG.debug("user_name=%s", user_name)
    # Get the tunnel info.
    info, _ = _get_tunnel_info(user_name)
    _LOG.info("info=%s", info)
    #
    for service_name, server, port in info:
        _LOG.info(
            "Stopping tunnel for service '%s' server=%s port=%s",
            service_name,
            server,
            port,
        )
        _kill_ssh_tunnel_process(port)


def _check_tunnels(user_name):
    """
    Check the status of the tunnels for the given user.
    """
    _LOG.info("user_name=%s", user_name)
    # Get the tunnel info.
    info, _ = _get_tunnel_info(user_name)
    _LOG.info("info=%s", info)
    #
    for service_name, server, port in info:
        pids = _get_ssh_tunnel_process(port)
        if pids:
            msg = "exists with pid=%s" % pids
        else:
            msg = "doesn't exist"
        _LOG.info(
            "service='%s' server=%s port=%s %s", service_name, server, port, msg
        )


def _kill_all_tunnel_processes():
    cmd = "ps ax | grep 'ssh -i' | grep localhost: | grep -v grep"
    pids, txt = _parse_ps_output(cmd)
    _LOG.info("Before killing all tunnel processes:\n%s", txt)
    #
    for pid in pids:
        os.kill(pid, signal.SIGKILL)
    _LOG.info("Killed %s processes", len(pids))
    #
    cmd = "ps ax | grep 'ssh -i' | grep localhost: | grep -v grep"
    pids, txt = _parse_ps_output(cmd)
    _LOG.info("After killing all tunnel processes:\n%s", txt)
    dbg.dassert_eq(len(pids), 0)


# ##############################################################################


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--user", type=str, action="store")
    parser.add_argument(
        "--action",
        required=True,
        action="store",
        choices="start stop check killall".split(),
    )
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
    if args.action == "start":
        _start_tunnels(user_name)
    elif args.action == "stop":
        _stop_tunnels(user_name)
    elif args.action == "check":
        _check_tunnels(user_name)
    elif args.action == "killall":
        _kill_all_tunnel_processes()
    else:
        dbg.dfatal("Invalid action='%s'" % args.action)


if __name__ == "__main__":
    _main()
