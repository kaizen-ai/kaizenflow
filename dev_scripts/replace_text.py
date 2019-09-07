#!/usr/bin/env python
"""
Replace an instance of text in all py, ipynb, and txt files.

> replace_text.py --old "import core.finance" --new "import core.finance" --preview
> replace_text.py --old "alphamatic/kibot/All_Futures" --new "alphamatic/kibot/All_Futures" --preview
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.printing as printing
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _look_for(file_name, args):
    txt = io_.from_file(file_name, split=False)
    txt = txt.split("\n")
    res = []
    found = False
    for i, line in enumerate(txt):
        if args.old in line:
            # ./install/create_conda.py:21:import helpers.helper_io as io_
            res.append("%s:%s:%s" % (file_name, i + 1, line))
            found = True
    return found, res


def _replace_with_perl(file_name, args):
    perl_opts = []
    perl_opts.append("-p")
    if args.backup:
        perl_opts.append("-i.bak")
    else:
        perl_opts.append("-i")
    if "/" in args.old or "/" in args.new:
        sep = "|"
    else:
        sep = "/"
    # s|\\balphamatic/kibot/All_Futures|alphamatic/kibot/All_Futures|g
    regex = ""
    regex += "s" + sep
    # regex += "\\b"
    regex += args.old
    regex += sep
    regex += args.new
    regex += sep
    regex += "g"
    # regex = r"s%s\\b%s%s%s%sg" % (sep, args.old, sep, args.new, sep)
    if True:
        perl_opts.append("-e '%s'" % regex)
    else:
        perl_opts.append(r"-e '%s unless /^\s*#/'" % regex)
    cmd = "perl %s %s" % (" ".join(perl_opts), file_name)
    si.system(cmd, suppress_output=False)


def _main(args):
    dbg.init_logger(args.log_level)
    dirs = args.dirs
    #
    exts = args.ext
    # exts = "py,ipynb,txt"
    exts = exts.split(",")
    _LOG.info("Extensions: %s", exts)
    file_names = []
    for d in dirs:
        _LOG.info("Processing dir '%s'", d)
        for ext in exts:
            file_names_tmp = io_.find_files(d, "*." + ext)
            _LOG.info("Found %s files", len(file_names_tmp))
            file_names.extend(file_names_tmp)
    file_names = [f for f in file_names if ".ipynb_checkpoints" not in f]
    _LOG.info("Found %s target files", len(file_names))
    # Look for files with values.
    res = []
    file_names_to_process = []
    for f in file_names:
        found, res_tmp = _look_for(f, args)
        _LOG.debug("File='%s', found=%s, res_tmp=\n%s", f, found, res_tmp)
        res.extend(res_tmp)
        if found:
            file_names_to_process.append(f)
    #
    txt = "\n".join(res)
    _LOG.info("Found %s occurrences\n%s", len(res), printing.space(txt))
    _LOG.info("Found %s files to process", len(file_names_to_process))
    io_.to_file("./cfile", txt)
    #
    if args.preview:
        _LOG.warning("Preview only as required. Results saved in ./cfile")
        exit(0)
    # Replace.
    _LOG.info(
        "Found %s files:\n%s",
        len(file_names_to_process),
        printing.space("\n".join(file_names_to_process)),
    )
    for file_name in file_names_to_process:
        _LOG.info("* Processing %s", file_name)
        _replace_with_perl(file_name, args)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--old",
        action="store",
        type=str,
        required=True,
        help="regex (in perl format) to replace",
    )
    parser.add_argument(
        "--new",
        action="store",
        type=str,
        required=True,
        help="regex (in perl format) to use",
    )
    parser.add_argument(
        "--preview", action="store_true", help="Preview only the replacement"
    )
    parser.add_argument(
        "--ext",
        action="store",
        type=str,
        default="py,ipynb",
        help="Extensions to process",
    )
    parser.add_argument(
        "--backup", action="store_true", help="Keep backups of files"
    )
    parser.add_argument(
        "--dirs",
        action="append",
        type=str,
        default=".",
        help="Directories to process",
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
    _main(args)


if __name__ == "__main__":
    _parse()
