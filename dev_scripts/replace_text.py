#!/usr/bin/env python
"""
Replace an instance of text in all py, ipynb, and txt files.

> replace_text.py --old io_ --new io_
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.printing as printing
import helpers.system_interaction as hsi

_LOG = logging


def _look_for(file_name, args):
    io_.
    


def _replace_with_perl(file_name, args):
    perl_opts = []
    perl_opts.append("-p")
    if args.backup:
        perl_opts.append("-i.bak")
    else:
        perl_opts.append("-i")
    if True:
        perl_opts.append("-e 's/\\b%s/%s/g'" % (args.old, args.new))
    else:
        perl_opts.append(
            r"-e 's/\\b%s/%s/g unless /^\s*#/'" % (args.old, args.new))
    cmd = "perl %s %s" % (" ".join(perl_opts), file_name)
    hsi.system(cmd, suppress_output=False)


def _main(args):
    dbg.init_logger(args.log_level)
    dirs = args.dirs
    # TODO(gp): Fix this.
    is_mac = True
    if is_mac:
        # Mac.
        cmd = (r'find %s '
               r'\( -name "*.ipynb" -o -name "*.py" -o -name "*.txt" '
               r'\)' % dirs)
    else:
        # Linux.
        cmd = r'find %s -regex ".*\.\(ipynb\|py\|txt\)"' % dirs
    _, txt = hsi.system_to_string(cmd)
    file_names = [f for f in txt.split("\n") if f != ""]
    file_names = [f for f in file_names if ".ipynb_checkpoints" not in f]
    _LOG.info("Found %s files", len(file_names))
    #
    for f in file_names:


    grep_opts = ""
    if args.preview:
        # Print file name and line numbers.
        grep_opts += " -n"
    else:
        grep_opts += " -l"
    cmd += ' | xargs -0 grep %s "%s"' % (grep_opts, args.old)
    # Custom filtering.
    if args.preview:
        cmd = cmd + " | tee cfile"
        hsi.system(cmd, suppress_output=False)
        _log.warning("Preview only as required. Results saved in ./cfile")
        exit(0)
    # Find files.
    rc, output = hsi.system_to_string(cmd, abort_on_error=False)
    if rc != 0:
        _LOG.warning("Can't find any file to process")
    # Replace.
    file_names = [f for f in output.split("\n") if f != ""]
    _LOG.info("Found %s files:\n%s", len(file_names),
              printing.space("\n".join(file_names)))
    for file_name in file_names:
        _LOG.info("* Processing " + file_name)
        _replace_with_perl(file_name, args)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--old",
        action="store",
        type=str,
        required=True,
        help="regex (in perl format) to replace")
    parser.add_argument(
        "--new",
        action="store",
        type=str,
        required=True,
        help="regex (in perl format) to use")
    parser.add_argument(
        "--preview", action="store_true", help="Preview only the replacement")
    parser.add_argument(
        "--backup", action="store_true", help="Keep backups of files")
    parser.add_argument(
        "--dirs",
        action="append",
        type=str,
        default=".",
        help="Directories to process")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    args = parser.parse_args()
    _main(args)


if __name__ == "__main__":
    _parse()
