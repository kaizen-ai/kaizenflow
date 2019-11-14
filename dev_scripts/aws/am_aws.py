#!/usr/bin/env python
"""
Start / stop / check AWS instance.
"""

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _get_instance_ip():
    cmd = "aws/get_inst_ip.sh"
    _, txt = si.system_to_string(cmd)
    txt = txt.rstrip("\n")
    return txt


def _get_instance_id():
    return os.environ["AM_INST_ID"]


def _gest_inst_status():
    # Get status.
    cmd = "get_inst_status.sh"
    _, txt = si.system_to_string(cmd)
    if txt:
        # INSTANCESTATUSES        us-east-1a      i-07f9b5323aa7a2ff2
        # INSTANCESTATE   16      running
        # INSTANCESTATUS  ok
        # DETAILS reachability    passed
        # SYSTEMSTATUS    ok
        # DETAILS reachability    passed
        res = None
        for l in txt.split("\n"):
            if l.startswith("INSTANCESTATE"):
                res = l.split()[2]
                break
        dbg.dassert_is_not(res, None)
    else:
        res = "stopped"
    return res


def _main(parser):
    args = parser.parse_args()
    inst_id = _get_instance_id()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    if args.action == "start":
        status = _gest_inst_status()
        _LOG.info("Current instance status: %s", status)
        if status == "stopped":
            cmd = "aws ec2 start-instances --instance-ids %s" % inst_id
            si.system(cmd)
            cmd = "aws ec2 wait instance-running --instance-ids %s" % inst_id
            si.system(cmd)
        else:
            _LOG.warning("Nothing to do")
        _LOG.info("status=%s", status)
        ip = _get_instance_ip()
        _LOG.info("IP: %s", ip)
    elif args.action == "stop":
        status = _gest_inst_status()
        _LOG.info("Current instance status: %s", status)
        if status == "running":
            cmd = "aws ec2 stop-instances --instance-ids %s" % inst_id
            si.system(cmd)
            cmd = "aws ec2 wait instance-stopped --instance-ids %s" % inst_id
            si.system(cmd)
        else:
            _LOG.warning("Nothing to do")
        #
        status = _gest_inst_status()
        dbg.dassert_eq(status, "stopped")


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--action",
        action="store",
        choices=["start", "stop"],
        required=True,
        help="Select action to execute",
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    #
    _main(parser)


if __name__ == "__main__":
    _parse()
