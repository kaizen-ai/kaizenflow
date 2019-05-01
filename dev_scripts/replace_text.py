#!/usr/bin/env python
"""
Replace an instance of text in all py, ipynb, and txt files.

# Use cases.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.system_interaction as hsi

_log = logging


def _main(args):
    dbg.init_logger2(args.log_level)
    dirs = args.dirs
    # TODO(gp): Fix this.
    is_mac = True
    if is_mac:
        # Mac.
        cmd = (r'find %s '
               r'\( -name "*.ipynb" -o -name "*.py" -o -name "*.txt" '
               r'\) -print0' % dirs)
    else:
        # Linux.
        cmd = r'find %s -regex ".*\.\(ipynb\|py\|txt\)" -print0' % dirs
    #
    use_ack = False
    if use_ack:
        # Use ack.
        ack_opts = "--nogroup --nocolor"
        if args.preview:
            # Print file name and line numbers.
            ack_opts += " -H"
        else:
            # Print only file name.
            ack_opts += " -l"
        cmd += ' | xargs -0 ack %s "%s"' % (ack_opts, args.old)
    else:
        # Use grep.
        grep_opts = ""
        if args.preview:
            # Print file name and line numbers.
            grep_opts += " -n"
        else:
            grep_opts += " -l"
        cmd += ' | xargs -0 grep %s "%s"' % (grep_opts, args.old)
    # Custom filtering.
    if False:
        cmd += r" | \grep -v _compute_derived_features"
    #
    if args.preview:
        cmd = cmd + " | tee cfile"
        hsi.system(cmd, suppress_output=False)
        _log.warning("Preview only as required. Results saved in ./cfile")
        exit(0)
    # Find files.
    rc, output = hsi.system_to_string(cmd, abort_on_error=False)
    if rc != 0:
        _log.warning("Can't find any instance of test in the code")
    # Replace.
    file_names = [f for f in output.split("\n") if f != ""]
    _log.info("Found %s files:\n%s" % (len(file_names), "\n".join(file_names)))
    for file_name in file_names:
        _log.info("* " + file_name)
        perl_opts = []
        perl_opts.append("-p")
        if not args.no_backup:
            perl_opts.append("-i.bak")
        else:
            perl_opts.append("-i")
        #perl_opts.append("-e 's/\\b%s/%s/g'" % (args.old, args.new))
        perl_opts.append(
            r"-e 's/\\b%s/%s/g unless /^\s*#/'" % (args.old, args.new))
        cmd = "perl %s %s" % (" ".join(perl_opts), file_name)
        hsi.system(cmd, suppress_output=False)


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
        "--no_backup", action="store_true", help="Keep backups of files")
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
