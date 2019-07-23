#!/usr/bin/env python
"""
"""

import argparse
import functools
import logging
import os

import bs4
import numpy as np
import pandas as pd
import tqdm
from joblib import Parallel, delayed

import helpers.dbg as dbg
import helpers.helper_io as io_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

@functools.lru_cache(maxsize=None)
def _get_instance_ip():
    cmd = "aws/get_instance_ip.sh"
    _, txt = si.system_to_string(cmd)
    txt = txt.rstrip("\n")
    return txt


@functools.lru_cache(maxsize=None)
def _get_instance_id()
    return os.environ["INST_ID"]


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    if args.action == "start":
        cmd = "aws ec2 start-instances --instance-ids $INST_ID && aws ec2 wait instance-running --instance-ids $INST_ID



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--action",
        action="store",
        choices=[
            "start",
            "stop",
            "get_ip",
        ],
        required=True,
        help="Select the dataset to download")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    _main(parser)
