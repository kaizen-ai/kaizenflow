#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.

Check dev_scripts/linter.py to see an example of a script using this template.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.io_ as io_
#import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--dst_dir',
        action="store",
        help="Destination dir",
        required=True)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    temp_dir = "./tmp.gdoc_backup"
    io_.create_dir(temp_dir, incremental=True)
    dst_dir = args.dst_dir
    io_.create_dir(temp_dir, incremental=True)
    tar_name=$(date '+%Y-%m-%d_%H-%M')
    rm -rf $temp_dir/*
    rclone copy particle_research:Particle_research $temp_dir --drive-shared-with-me
        tar -cf $base$tar_name.tar.gz -C $temp_dir .
    rm -rf $temp_dir
    #find $base -type f -mtime +3 -delete


if __name__ == '__main__':
    _main(_parse())

