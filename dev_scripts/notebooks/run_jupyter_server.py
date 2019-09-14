#!/usr/bin/env python

"""
Start a jupyter process.
"""

import argparse
import logging
import os
import signal
import sys
import time

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _get_port_process(port):
    """
    Find the pids of the jupyter servers connected to a certain port.
    """

    def _keep_line(port, line):
        keep = (
            ("jupyter-notebook" in line)
            and ("-port" in line)
            and (str(port) in line)
        )
        return keep

    keep_line = lambda line: _keep_line(port, line)
    # filter_cmd = 'grep "jupyter-notebook" | grep "\-port" | grep %d' % port
    pids, _ = si.get_process_pids(keep_line)
    _LOG.debug("pids=%s", pids)
    return pids


def _kill_port_process(port):
    """
    Kill all the processes attached to a given port.
    """
    pids = _get_port_process(port)
    _LOG.info("Killing pids=%s", pids)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError as e:
            _LOG.warning(str(e))


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--port",
        action="store",
        default=None,
        help="Override the " "default port to use",
    )
    parser.add_argument(
        "--kill",
        action="store_true",
        help="Kill any jupyter process using the requested port",
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
    #
    pids = _get_port_process(jupyter_port)
    if pids:
        _LOG.warning("Found another jupyter notebook running on the same port")
        if args.kill:
            _kill_port_process(jupyter_port)
            _LOG.info("Waiting processes to die")
            cnt = 0
            max_cnt = 10
            while cnt < max_cnt:
                time.sleep(0.1)
                pids = _get_port_process(jupyter_port)
                cnt += 1
                if not pids:
                    break
            pids = _get_port_process(jupyter_port)
            dbg.dassert_eq(len(pids), 0)
        else:
            print(
                "Exiting: re-run with --kill to kill the processes squatting "
                "the port"
            )
            sys.exit(-1)
    #
    ip_name = "localhost"
    print("You can connect to: %s:%s" % (ip_name, jupyter_port))
    cmd = "jupyter notebook '--ip=*' --browser chrome . --port %s" % jupyter_port
    si.system(cmd, suppress_output=False)


if __name__ == "__main__":
    _main(_parse())
