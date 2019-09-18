#!/usr/bin/env python

"""
Start a jupyter server.

# Start a jupyter server killing the existing one:
> run_jupyter_server.py force_start
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _get_port_process(port):
    """
    Find the pids of the jupyter servers connected to a certain port.
    """

    # 'grep "jupyter-notebook" | grep "\-port" | grep %d' % port
    def _keep_line(port, line):
        keep = (
            ("jupyter-notebook" in line)
            and ("-port" in line)
            and (str(port) in line)
        )
        return keep

    keep_line = lambda line: _keep_line(port, line)
    pids, txt = si.get_process_pids(keep_line)
    _LOG.debug("pids=%s", pids)
    return pids, txt


def _check(port):
    pids, txt = _get_port_process(port)
    _LOG.info("Found %d pids: %s\n%s", len(pids), str(pids), "\n".join(txt))


def _kill(port):
    get_pids = lambda: _get_port_process(port)
    si.kill_process(get_pids)


def _start(port, action):
    pids, txt = _get_port_process(port)
    if pids:
        _LOG.warning(
            "Found other jupyter notebooks running on the same " "port:\n%s",
            "\n".join(txt),
        )
        if action == "force_start":
            _kill(port)
        else:
            print(
                "Exiting: re-run with force_start to kill the processes "
                "squatting the port"
            )
            return
    #
    ip_name = "localhost"
    print("You can connect to: %s:%s" % (ip_name, port))
    cmd = "jupyter notebook '--ip=*' --browser chrome . --port %s" % port
    if action != "only_print_cmd":
        si.system(cmd, suppress_output=False, log_level="echo")
    else:
        print(cmd)


# ##############################################################################


def _parse():
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
        choices=["start", "force_start", "check", "kill", "only_print_cmd"],
        help=_help,
    )
    parser.add_argument(
        "--port",
        action="store",
        default=None,
        help="Override the " "default port to use",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    # Get the default port.
    credentials = usc.get_credentials()
    jupyter_port = credentials["jupyter_port"]
    dbg.dassert_is_not(jupyter_port, None)
    if args.port is not None:
        port = int(args.port)
        _LOG.warning("Overriding port %d with port %d", jupyter_port, port)
        jupyter_port = port
    _LOG.info("Target port='%d'", jupyter_port)
    #
    action = args.positional[0]
    _LOG.debug("action=%s", action)
    if action == "check":
        _check(jupyter_port)
    elif action == "kill":
        _kill(jupyter_port)
    elif action in ("start", "force_start", "only_print_cmd"):
        _start(jupyter_port, action)
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
