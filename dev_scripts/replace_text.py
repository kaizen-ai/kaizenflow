#!/usr/bin/env python

r"""
- Replace an instance of text in all py, ipynb, and txt files or in filenames.
- Git rename the names of files based on certain criteria.

# Replace an import with a new one:
> replace_text.py \
        --old "import core.fin" \
        --new "import core.finance" \
        --preview

# Custom flow:
> replace_text.py --custom_flow _custom1

# Custome flow for AmpTask14
> replace_text.py --custom_flow _custom2 --revert_all

# Replace text in a specific directory:
> replace_text.py \
        --old "exec " \
        --new "execute " \
        --preview \
        --dirs dev_scripts \
        --exts None


# To revert all files but this one:
> gs -s | \
        grep -v dev_scripts/replace_text.py | \
        grep -v "\?" | \
        awk '{print $2}' | \
        xargs git checkout --
"""

import argparse
import logging
import os
import pprint
import re
import sys
from typing import Dict, List, Optional, Tuple

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import helpers.printing as hprint
import helpers.system_interaction as hsyste

# TODO(gp): 
#  - allow to read a cfile with a subset of files / points to replace
#  - allow to work with no filter
#  - fix the help
#  - figure out the issue with encoding

_LOG = logging.getLogger(__name__)

_ENCODING = "ISO-8859-1"


def _get_all_files(dirs: List[str], exts: Optional[List[str]]) -> List[str]:
    """
    Find all the files with the given extensions in files under `dirs`.

    :param exts: if None, no filtering by extensions
    """
    if exts is not None:
        # Extensions are specified.
        dbg.dassert_isinstance(exts, list)
        dbg.dassert_lte(1, len(exts))
        for ext in exts:
            dbg.dassert(not ext.startswith("."), "Invalid ext='%s'", ext)
    # Get files.
    _LOG.debug("exts=%s", exts)
    file_names = []
    for d in dirs:
        _LOG.debug("Processing dir '%s'", d)
        if exts is not None:
            # Extensions are specified: find all the files with the given extensions.
            for ext in exts:
                file_names_tmp = hio.find_files(d, "*." + ext)
                _LOG.debug("ext=%s -> found %s files", ext, len(file_names_tmp))
                file_names.extend(file_names_tmp)
        else:
            # No extension: find all files.
            file_names_tmp = hio.find_files(d, "*")
            _LOG.debug("exts=%s -> found %s files", exts, len(file_names_tmp))
            file_names.extend(file_names_tmp)
    # Exclude some files.
    file_names = [f for f in file_names if ".ipynb_checkpoints" not in f]
    file_names = [f for f in file_names if "__pycache__" not in f]
    file_names = [f for f in file_names if ".mypy_cache" not in f]
    file_names = [f for f in file_names if ".pytest_cache" not in f]
    file_names = [f for f in file_names if "replace_text.py" not in f]
    file_names = [f for f in file_names if ".git/" not in f]
    file_names = [f for f in file_names if os.path.basename(f) != "cfile"]
    _LOG.info(
        "Found %s target files with extension '%s'", len(file_names), str(exts)
    )
    return file_names


# #############################################################################
# Replace strings inside file.
# #############################################################################


def _get_files_to_replace(
    file_names: List[str], old_regex: str
) -> Tuple[List[str], str]:
    """
    Return the list of files that contain `old_regex` and the corresponding
    cfile.
    """
    # Look for files with values.
    res = []
    file_names_to_process = []
    for f in file_names:
        found, res_tmp = _look_for(f, old_regex)
        _LOG.debug("File='%s', found=%s, res_tmp=\n%s", f, found, res_tmp)
        res.extend(res_tmp)
        if found:
            file_names_to_process.append(f)
    #
    txt = "\n".join(res)
    _LOG.info("Found %s occurrences\n%s", len(res), hprint.indent(txt))
    _LOG.info("Found %s files to process", len(file_names_to_process))
    return file_names_to_process, txt


def _look_for(file_name: str, old_regex: str) -> Tuple[bool, List[str]]:
    """
    Look for `old_regex` in `file_name` returning if it was found and the
    corresponding cfile entry.
    """
    txt = hio.from_file(file_name, encoding=_ENCODING)
    txt = txt.split("\n")
    res = []
    found = False
    for i, line in enumerate(txt):
        m = re.search(old_regex, line)
        if m:
            # ./install/create_conda.py:21:import helpers.helper_io as hio
            res.append("%s:%s:%s" % (file_name, i + 1, line))
            found = True
    return found, res


# #############################################################################


def _replace_with_perl(
    file_name: str, old_regex: str, new_regex: str, backup: bool
) -> None:
    perl_opts = []
    perl_opts.append("-p")
    if backup:
        perl_opts.append("-i.bak")
    else:
        perl_opts.append("-i")
    if "/" in old_regex or "/" in new_regex:
        sep = "|"
    else:
        sep = "/"
    # s|\\balphamatic/kibot/All_Futures|alphamatic/kibot/All_Futures|g
    regex = ""
    regex += "s" + sep
    # regex += "\\b"
    regex += old_regex
    regex += sep
    regex += new_regex
    regex += sep
    regex += "g"
    # regex = r"s%s\\b%s%s%s%sg" % (sep, args.old, sep, args.new, sep)
    if True:
        perl_opts.append("-e '%s'" % regex)
    else:
        perl_opts.append(r"-e '%s unless /^\s*#/'" % regex)
    cmd = "perl %s %s" % (" ".join(perl_opts), file_name)
    hsyste.system(cmd, suppress_output=False)


def _replace_with_python(
    file_name: str, old_regex: str, new_regex: str, backup: bool
) -> None:
    if backup:
        cmd = "cp %s %s.bak" % (file_name, file_name)
        hsyste.system(cmd)
    #
    lines = hio.from_file(file_name, encoding=_ENCODING).split("\n")
    lines_out = []
    for line in lines:
        _LOG.debug("line='%s'", line)
        line_new = re.sub(old_regex, new_regex, line)
        if line_new != line:
            _LOG.debug("    -> line_new='%s'", line_new)
        lines_out.append(line_new)
    lines_out = "\n".join(lines_out)
    hio.to_file(file_name, lines_out)


def _replace(
    file_names_to_process: List[str],
    old_regex: str,
    new_regex: str,
    backup: bool,
    mode: str,
) -> None:
    """
    Replace `old_regex` with `new_regex` in the given files using perl or
    Python.

    :param backup: make a backup of the file before the replacement
    :param mode: `replace_with_perl` or `replace_with_python`
    """
    _LOG.info(
        "Found %s files:\n%s",
        len(file_names_to_process),
        hprint.indent("\n".join(file_names_to_process)),
    )
    for file_name in file_names_to_process:
        _LOG.info("* _replace %s", file_name)
        if mode == "replace_with_perl":
            _replace_with_perl(file_name, old_regex, new_regex, backup)
        elif mode == "replace_with_python":
            _replace_with_python(file_name, old_regex, new_regex, backup)
        else:
            raise ValueError("Invalid mode='%s'" % mode)


def _replace_repeated_lines(file_name: str, new_regex: str) -> None:
    """
    Remove consecutive lines in `file_name` that are equal and contain
    `new_regex`.

    This is equivalent to Linux `uniq`.
    """
    _LOG.info("* _replace_repeated_lines %s", file_name)
    lines = hio.from_file(file_name, encoding=_ENCODING).split("\n")
    lines_out = []
    prev_line = None
    for line in lines:
        _LOG.debug("line='%s'", line)
        if new_regex not in line:
            # Emit.
            lines_out.append(line)
        else:
            if line != prev_line:
                # Emit.
                lines_out.append(line)
            else:
                _LOG.debug("    -> skipped line")
        prev_line = line
    lines_out = "\n".join(lines_out)
    hio.to_file(file_name, lines_out)


# #############################################################################


def _custom1(args: argparse.Namespace) -> None:
    to_replace = [
        # ("import helpers.printing as hprint", "import helpers.printing as
        # pri"),
        # (r"printing\.", "hprint."),
        ("import helpers.config", "import core.config")
    ]
    dirs = ["."]
    exts = ["py", "ipynb"]
    backup = args.backup
    preview = args.preview
    mode = "replace_with_python"
    #
    file_names = _get_all_files(dirs, exts)
    txt = ""
    for old_regex, new_regex in to_replace:
        print(hprint.frame("%s -> %s" % (old_regex, new_regex)))
        file_names_to_process, txt_tmp = _get_files_to_replace(
            file_names, old_regex
        )
        dbg.dassert_lte(1, len(file_names_to_process))
        # Replace.
        if preview:
            txt += txt_tmp
        else:
            _replace(file_names_to_process, old_regex, new_regex, backup, mode)
    hio.to_file("./cfile", txt)
    if preview:
        _LOG.warning("Preview only as required. Results saved in ./cfile")
        sys.exit(0)


def _fix_AmpTask1403_helper(
    args: argparse.Namespace, to_replace: List[Tuple[str, str]]
) -> None:
    dirs = ["."]
    exts = ["py", "ipynb"]
    backup = args.backup
    preview = args.preview
    mode = "replace_with_python"
    #
    file_names = _get_all_files(dirs, exts)
    # file_names = ["amp/core/dataflow/nodes/test/test_volatility_models.py"]
    # Store the replacement points.
    txt = ""
    for old_regex, new_regex in to_replace:
        print(hprint.frame("%s -> %s" % (old_regex, new_regex)))
        file_names_to_process, txt_tmp = _get_files_to_replace(
            file_names, old_regex
        )
        # dbg.dassert_lte(1, len(file_names_to_process))
        if len(file_names_to_process) < 1:
            _LOG.warning("Didn't find files to replace")
        # Replace.
        if preview:
            txt += txt_tmp
        else:
            _replace(file_names_to_process, old_regex, new_regex, backup, mode)
            for file_name in file_names_to_process:
                _replace_repeated_lines(file_name, new_regex)
    hio.to_file("./cfile", txt)
    if preview:
        _LOG.warning("Preview only as required. Results saved in ./cfile")
        sys.exit(0)


def _fix_AmpTask1403(args: argparse.Namespace) -> None:
    """
    Implement AmpTask1403.
    """
    # From longest to shortest to avoid nested replacements.
    to_replace = [
        "import core.config as cfg",
        "import core.config as ccfg",
        #"import core.config as cconfi",
        "import core.config as cconfig",
        "import core.config_builders as ccbuild",
        "import core.config_builders as cfgb",
    ]
    #to_replace = [(f"^{s}$", "import core.config as cconfig") for s in to_replace]
    to_replace = [(f"{s}", "import core.config as cconfig") for s in to_replace]
    _fix_AmpTask1403_helper(args, to_replace)
    #
    to_replace = [
        # (r"printing\.", "hprint."),
        # ("import helpers.config", "import core.config")
        "ccfg",
        #"cconfi",
        "cconfig",
        "cfg",
        "ccbuild",
        "cfgb",
    ]
    to_replace = [(rf"{s}\.", "cconfig.") for s in to_replace]
    _fix_AmpTask1403_helper(args, to_replace)


# #############################################################################
# Rename files.
# #############################################################################


def _get_files_to_rename(
    file_names: List[str], old_regex: str, new_regex: str
) -> Tuple[List[str], Dict[str, str]]:
    # Look for files containing "old_regex".
    file_map: Dict[str, str] = {}
    file_names_to_process = []
    for f in file_names:
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        found = old_regex in basename
        if found:
            new_basename = basename.replace(old_regex, new_regex)
            _LOG.debug("File='%s', found=%s", f, found)
            file_names_to_process.append(f)
            file_map[f] = os.path.join(dirname, new_basename)
    #
    _LOG.info("Found %s files to process", len(file_names_to_process))
    _LOG.info("%s", pprint.pformat(file_map))
    return file_names_to_process, file_map


def _rename(file_names_to_process: List[str], file_map: Dict[str, str]) -> None:
    for f in file_names_to_process:
        new_name = file_map[f]
        cmd = "git mv %s %s" % (f, new_name)
        hsyste.system(cmd)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--revert_all",
        action="store_true",
        help="Revert all the files before processing",
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
        "--action",
        action="store",
        default="replace",
        choices=["replace", "rename"],
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
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(args.log_level)
    if args.revert_all:
        # Revert all the files but this one. Use at your own risk.
        _LOG.warning("Reverting all files but this one")
        cmd = [
            r"git status -s",
            r"grep -v dev_scripts/replace_text.py",
            r'grep -v "\?"',
            r"awk '{print $2}'",
            r"xargs git checkout --",
        ]
        cmd = " | ".join(cmd)
        print(f"> {cmd}")
        hsyste.system(cmd)
    if args.custom_flow:
        eval("%s(args)" % args.custom_flow)
    else:
        # Use command line params.
        dirs = args.dirs
        if dirs is None:
            dirs = ["."]
        _LOG.info("dirs=%s", dirs)
        # Parse the extensions.
        if args.ext == "None":
            exts = None
        else:
            exts = args.ext
            # exts = "py,ipynb,txt"
            exts = exts.split(",")
        _LOG.info("extensions=%s", exts)
        # Find all the files with the correct extension.
        file_names = _get_all_files(dirs, exts)
        if args.action == "replace":
            # Replace.
            file_names_to_process, txt = _get_files_to_replace(
                file_names, args.old
            )
            #
            if args.preview:
                hio.to_file("./cfile", txt)
                _LOG.warning("Preview only as required. Results saved in ./cfile")
                sys.exit(0)
            _replace(
                file_names_to_process, args.old, args.new, args.backup, args.mode
            )
        elif args.action == "rename":
            # Rename.
            file_names_to_process, file_map = _get_files_to_rename(
                file_names, args.old, args.new
            )
            if args.preview:
                _LOG.warning("Preview only as required.")
                sys.exit(0)
            _rename(file_names_to_process, file_map)
        else:
            raise ValueError("Invalid action='%s'" % args.action)


if __name__ == "__main__":
    _main(_parse())
