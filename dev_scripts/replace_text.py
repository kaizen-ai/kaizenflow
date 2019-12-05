#!/usr/bin/env python

r"""
Replace an instance of text in all py, ipynb, and txt files.

> replace_text.py --old "import core.finance" --new "import core.finance" --preview
> replace_text.py --old "alphamatic/kibot/All_Futures" --new "alphamatic/kibot/All_Futures" --preview

# Custom flow:
> replace_text.py --custom_flow _custom1

# Replace in scripts
> replace_text.py --old "exec " --new "execute " --preview --dirs dev_scripts --exts None

# To revert all files but this one
> gs -s | grep -v dev_scripts/replace_text.py | grep -v "\?" | awk '{print $2}' | xargs git checkout --
"""

import argparse
import logging
import re

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

# TODO(gp):
#  - allow to read a cfile with a subset of files / points to replace
#  - allow to work with no filter
#  - fix the help
#  - figure out the issue with encoding

_LOG = logging.getLogger(__name__)

_ENCODING = "ISO-8859-1"


def _get_extensions(exts):
    # exts = "py,ipynb,txt"
    exts = exts.split(",")
    _LOG.info("Extensions: %s", exts)
    return exts


def _get_file_names(old_string, dirs, exts):
    """
    :param exts: if None, no filtering by extentions
    """
    if exts is not None:
        dbg.dassert_isinstance(exts, list)
        dbg.dassert_lte(1, len(exts))
        for ext in exts:
            dbg.dassert(not ext.startswith("."), "Invalid ext='%s'", ext)
    # Get files.
    _LOG.debug("exts=%s", exts)
    file_names = []
    for d in dirs:
        _LOG.debug("Processing dir '%s'", d)
        if exts is None:
            for ext in exts:
                file_names_tmp = io_.find_files(d, "*." + ext)
                _LOG.debug("ext=%s -> found %s files", ext, len(file_names_tmp))
        else:
            file_names_tmp = io_.find_files(d, "*")
            _LOG.debug("ext=%s -> found %s files", ext, len(file_names_tmp))
            file_names.extend(file_names_tmp)
    file_names = [f for f in file_names if ".ipynb_checkpoints" not in f]
    file_names = [f for f in file_names if "replace_text.py" not in f]
    _LOG.info("Found %s target files", len(file_names))
    # Look for files with values.
    res = []
    file_names_to_process = []
    for f in file_names:
        found, res_tmp = _look_for(f, old_string)
        _LOG.debug("File='%s', found=%s, res_tmp=\n%s", f, found, res_tmp)
        res.extend(res_tmp)
        if found:
            file_names_to_process.append(f)
    #
    txt = "\n".join(res)
    _LOG.info("Found %s occurrences\n%s", len(res), pri.space(txt))
    _LOG.info("Found %s files to process", len(file_names_to_process))
    return file_names_to_process, txt


def _look_for(file_name, old_string):
    txt = io_.from_file(file_name, encoding=_ENCODING)
    txt = txt.split("\n")
    res = []
    found = False
    for i, line in enumerate(txt):
        m = re.search(old_string, line)
        if m:
            # ./install/create_conda.py:21:import helpers.helper_io as io_
            res.append("%s:%s:%s" % (file_name, i + 1, line))
            found = True
    return found, res


# #############################################################################


def _replace_with_perl(file_name, old_string, new_string, backup):
    perl_opts = []
    perl_opts.append("-p")
    if backup:
        perl_opts.append("-i.bak")
    else:
        perl_opts.append("-i")
    if "/" in old_string or "/" in new_string:
        sep = "|"
    else:
        sep = "/"
    # s|\\balphamatic/kibot/All_Futures|alphamatic/kibot/All_Futures|g
    regex = ""
    regex += "s" + sep
    # regex += "\\b"
    regex += old_string
    regex += sep
    regex += new_string
    regex += sep
    regex += "g"
    # regex = r"s%s\\b%s%s%s%sg" % (sep, args.old, sep, args.new, sep)
    if True:
        perl_opts.append("-e '%s'" % regex)
    else:
        perl_opts.append(r"-e '%s unless /^\s*#/'" % regex)
    cmd = "perl %s %s" % (" ".join(perl_opts), file_name)
    si.system(cmd, suppress_output=False)


def _replace_with_python(file_name, old_string, new_string, backup):
    if backup:
        cmd = "cp %s %s.bak" % (file_name, file_name)
        si.system(cmd)
    #
    lines = io_.from_file(file_name, encoding=_ENCODING).split("\n")
    lines_out = []
    for line in lines:
        _LOG.debug("line='%s'", line)
        line_new = re.sub(old_string, new_string, line)
        if line_new != line:
            _LOG.debug("    -> line_new='%s'", line_new)
        lines_out.append(line_new)
    lines_out = "\n".join(lines_out)
    io_.to_file(file_name, lines_out)


def _replace(file_names_to_process, old_string, new_string, backup, mode):
    _LOG.info(
        "Found %s files:\n%s",
        len(file_names_to_process),
        pri.space("\n".join(file_names_to_process)),
    )
    for file_name in file_names_to_process:
        _LOG.info("* Processing %s", file_name)
        if mode == "replace_with_perl":
            _replace_with_perl(file_name, old_string, new_string, backup)
        elif mode == "replace_with_python":
            _replace_with_python(file_name, old_string, new_string, backup)
        else:
            raise ValueError("Invalid mode='%s'" % mode)


# #############################################################################


def _custom1(args):
    to_replace = [
        # ("import helpers.printing as printing", "import helpers.printing as
        # pri"),
        # (r"printing\.", "pri."),
        ("import helpers.config", "import core.config")
    ]
    dirs = "."
    exts = ["py", "ipynb"]
    backup = args.backup
    preview = args.preview
    mode = "replace_with_python"
    #
    txt = ""
    for old_string, new_string in to_replace:
        print(pri.frame("%s -> %s" % (old_string, new_string)))
        file_names_to_process, txt_tmp = _get_file_names(old_string, dirs, exts)
        dbg.dassert_lte(1, len(file_names_to_process))
        # Replace.
        if preview:
            txt += txt_tmp
        else:
            _replace(file_names_to_process, old_string, new_string, backup, mode)
    io_.to_file("./cfile", txt)
    if preview:
        _LOG.warning("Preview only as required. Results saved in ./cfile")
        exit(0)


# #############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--custom_flow", action="store", type=str)
    parser.add_argument(
        "--old",
        action="store",
        type=str,
        help="regex (in perl format) to replace",
    )
    parser.add_argument(
        "--new", action="store", type=str, help="regex (in perl format) to use"
    )
    parser.add_argument(
        "--preview", action="store_true", help="Preview only the replacement"
    )
    parser.add_argument(
        "--mode",
        action="store",
        default="replace_with_python",
        choices=["replace_with_python", "replace_with_perl"],
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
        "-d",
        "--dirs",
        action="append",
        default=None,
        help="Directories to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(args.log_level)
    if args.custom_flow:
        eval("%s(args)" % args.custom_flow)
    else:
        # Use command line params.
        dirs = args.dirs
        if dirs is None:
            dirs = ["."]
        _LOG.info("dirs=%s", dirs)
        if args.ext == "None":
            exts = None
        else:
            exts = _get_extensions(args.ext)
        _LOG.info("extensions=%s", exts)
        file_names_to_process, txt = _get_file_names(args.old, dirs, exts)
        io_.to_file("./cfile", txt)
        #
        if args.preview:
            _LOG.warning("Preview only as required. Results saved in ./cfile")
            exit(0)
        # Replace.
        _replace(
            file_names_to_process, args.old, args.new, args.backup, args.mode
        )


if __name__ == "__main__":
    _main(_parse())
