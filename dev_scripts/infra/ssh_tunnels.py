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
import helpers.system_interaction as hsi
import infra.ssh_config as ssh_cfg

_LOG = logging.getLogger(__name__)

# #############################################################################

# Servers
DEV_SERVER = '54.172.40.4'
DB_SERVER = ''

# Server ports
MAPPING = {
    "NGINX": ("Publish notebook server", DEV_SERVER, 8181),
    "NETDATA": ("System performance", DEV_SERVER, 19999),
    "JENKINS": ("Continuous integration", DEV_SERVER, 8111),
    "UPSOURCE": ("Code review", DEV_SERVER, 8080),
}

DEFAULT_PORTS = sorted(MAPPING.keys())

# Users
DEFAULT_RSA_KEY = "~/.ssh/id_rsa"

USERS = {
    'gp': {
        'key_path':
        DEFAULT_RSA_KEY,
        # Jupyter notebooks running at /data/gp_wd/src/particle{1, 2}
        'ports': [("Jupyter1", DEV_SERVER, 9185),
                  ("Jupyter2", DEV_SERVER, 9186)]
}

# ##############################################################################

def _create_tunnel(server, port, user_name):
    _LOG.debug("server=%s port=%s user_name=%s", server, port, user_name)
    dbg.dassert_in(user_name, ssh_cfg.USERS)
    key_path = ssh_cfg.USERS[user_name]['key_path']
    key_path = os.path.expanduser(key_path)
    _LOG.debug("key_path=%s", key_path)
    dbg.dassert_exists(key_path)
    cmd = "ssh -i {key_path} -f -nNT -L {port}:localhost:{port} {user_name}@" + server
    cmd = cmd.format(user_name=user_name, key_path=key_path, port=port)
    hsi.system(cmd, blocking=False)


def _get_tunnel_info(user_name):
    """
    Return the list of services for a user. Each service is represented
    as a triple (symbolic name, IP, port).
    """
    info = [ssh_cfg.MAPPING[n] for n in ssh_cfg.DEFAULT_PORTS]
    info.extend(ssh_cfg.USERS[user_name]['ports'])
    return info


def _parse_ps_output(cmd):
    rc, txt = hsi.system_to_string(cmd, abort_on_error=False)
    _LOG.debug("txt=\n%s", txt)
    pids = []
    if rc == 0:
        for line in txt.split("\n"):
            _LOG.debug("line=%s", line)
            # pylint: disable=C0301
            # > ps ax | grep 'ssh -i' | grep localhost
            # 19417   ??  Ss     0:00.39 ssh -i /Users/gp/.ssh/id_rsa -f -nNT -L 19999:localhost:19999 gp@54.172.40.4
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
    _LOG.debug("port=%s", port)
    cmd = "ps ax | grep 'ssh -i' | grep localhost:{port} | grep -v grep".format(
        port=port)
    pids, txt = _parse_ps_output(cmd)
    if len(pids) > 1:
        _LOG.debug("Expect a single process, instead got:\n%s", txt)
    _LOG.debug("pids=%s", pids)
    return pids


def _kill_ssh_tunnel_process(port):
    pids = _get_ssh_tunnel_process(port)
    for pid in pids:
        os.kill(pid, signal.SIGKILL)


# ##############################################################################


def _start_tunnels(user_name):
    _LOG.debug("user_name=%s", user_name)
    info = _get_tunnel_info(user_name)
    for sym_name, server, port in info:
        pids = _get_ssh_tunnel_process(port)
        if not pids:
            _LOG.info("Starting tunnel for service '%s' server=%s port=%s",
                      sym_name, server, port)
            _create_tunnel(server, port, user_name)
        else:
            _LOG.warning(
                "Tunnel for service '%s' on port %s already exist: skipping",
                sym_name, port)


def _stop_tunnels(user_name):
    _LOG.debug("user_name=%s", user_name)
    info = _get_tunnel_info(user_name)
    for sym_name, server, port in info:
        _LOG.info("Stopping tunnel for service '%s' server=%s port=%s",
                  sym_name, server, port)
        _kill_ssh_tunnel_process(port)


def _check_tunnels(user_name):
    _LOG.info("user_name=%s", user_name)
    info = _get_tunnel_info(user_name)
    for sym_name, server, port in info:
        pids = _get_ssh_tunnel_process(port)
        if pids:
            msg = "exists with pid=%s" % pids
        else:
            msg = "doesn't exist"
        _LOG.info("Service='%s' server=%s port=%s %s", sym_name, server, port,
                  msg)


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
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--user', type=str, action='store')
    parser.add_argument(
        '--action',
        required=True,
        action='store',
        choices="start stop check killall".split())
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    if args.user:
        user = args.user
    else:
        _, user = hsi.system_to_string('whoami')
    _LOG.info("user=%s", user)
    dbg.dassert_in(
        user,
        ssh_cfg.USERS,
        msg='There is no user with name %s. Please add it to infra/ssh_config.py'
        % user)
    if args.action == "start":
        _start_tunnels(user)
    elif args.action == "stop":
        _stop_tunnels(user)
    elif args.action == "check":
        _check_tunnels(user)
    elif args.action == "killall":
        _kill_all_tunnel_processes()
    else:
        dbg.dfatal("Invalid action='%s'" % args.action)


if __name__ == "__main__":
    _main()
