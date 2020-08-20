#!/usr/bin/env python

"""
Transform the output of `diff -r --brief src_dir dst_dir` into a script using vimdiff.

# To clean up the crap in the dirs:
> git status --ignored
> git clean -fdx --dry-run

# Diff dirs:
> diff_to_vimdiff.py --src_dir /Users/saggese/src/commodity_research2/amp --dst_dir /Users/saggese/src/commodity_research3/amp
"""

import argparse
import logging
import os
import re

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _diff(src_dir, dst_dir):
    """
    Run a diff command between the two dirs and save the output in a file.
    """
    _LOG.debug("Comparing dirs %s %s", src_dir, dst_dir)
    dst_file = "/tmp/tmp.diff_to_vimdiff.txt"
    dbg.dassert_exists(src_dir)
    dbg.dassert_exists(dst_dir)
    cmd = "diff --brief -r %s %s >%s" % (src_dir, dst_dir, dst_file)
    # We don't abort since diff rc != 0 in case of differences, which is a
    # valid outcome.
    si.system(cmd, abort_on_error=False)
    _LOG.debug("Diff output saved in %s", dst_file)
    input_file = dst_file
    return input_file


def _get_symbolic_filepath(src_dir, dst_dir, file_name):
    """
    Transform a path like:
        /Users/saggese/src/commodity_research2/amp/vendors2/first_rate/utils.py
    into:
        $SRC_DIR/amp/vendors2/first_rate/utils.py
    """
    file_name = file_name.replace(src_dir, "$SRC_DIR")
    file_name = file_name.replace(dst_dir, "$DST_DIR")
    return file_name


def _parse_diff_output(input_file, src_dir, dst_dir, args):
    """
    Process the output of diff and creates a file with the corresponding kjj
    """
    output_file = args.output_file
    # Read.
    dbg.dassert_exists(input_file)
    _LOG.info("Reading '%s'", input_file)
    txt = io_.from_file(input_file)
    txt = txt.split("\n")
    # Process.
    out = []
    for line in txt:
        _LOG.debug("# line='%s'", line)
        if line == "":
            continue
        comment = None
        out_line = None
        if line.startswith("Only in "):
            # Only in /data/gp_wd/src/deploy_particle1/: cfile
            m = re.match(r"^Only in (\S+): (\S+)$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
            # Check.
            file_name = "%s/%s" % (m.group(1), m.group(2))
            dbg.dassert_exists(file_name)
            if args.only_diff_content:
                # We want to see only files with diff content, so skip this.
                pass
            else:
                # Comment.
                dir_ = _get_symbolic_filepath(src_dir, dst_dir, m.group(1))
                dirs = dir_.split("/")
                dir_ = dirs[0]
                file_ = os.path.join(
                    *dirs[1:],
                    _get_symbolic_filepath(src_dir, dst_dir, m.group(2))
                )
                if args.src_dir_name is not None:
                    dir_ = dir_.replace("$SRC_DIR", args.src_dir_name)
                if args.dst_dir_name is not None:
                    dir_ = dir_.replace("$DST_DIR", args.dst_dir_name)
                comment = "ONLY: %s in '%s'" % (file_, dir_)
                # Diff command.
                # out_line = "vim %s" % file_name
                if args.src_dir in file_name:
                    out_line = "vimdiff %s %s" % (
                        file_name,
                        file_name.replace(args.src_dir, args.dst_dir),
                    )
                else:
                    dbg.dassert_in(args.dst_dir, file_name)
                    out_line = "vimdiff %s %s" % (
                        file_name,
                        file_name.replace(args.dst_dir, args.src_dir),
                    )
        elif line.startswith("Files "):
            # Files
            #   /data/gp_wd/src/deploy_particle1/compustat/fiscal_calendar.py and
            #   /data/gp_wd/src/particle1/compustat/fiscal_calendar.py differ
            m = re.match(r"^Files (\S+) and (\S+) differ$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
            # Check.
            dbg.dassert_exists(m.group(1))
            dbg.dassert_exists(m.group(2))
            if args.only_diff_files:
                pass
            else:
                # Comment.
                file1 = _get_symbolic_filepath(src_dir, dst_dir, m.group(1))
                file1 = file1.replace("$SRC_DIR/", "")
                file2 = _get_symbolic_filepath(src_dir, dst_dir, m.group(2))
                file2 = file2.replace("$DST_DIR/", "")
                dbg.dassert_eq(file1, file2)
                comment = "DIFF: %s" % file1
                # Diff command.
                out_line = "vimdiff %s %s" % (m.group(1), m.group(2))
        else:
            dbg.dfatal("Invalid line='%s'" % line)
        _LOG.debug("# line='%s'", line)
        if not args.skip_comments:
            if comment:
                out.append("#       " + comment)
        if not args.skip_vim:
            if out_line:
                _LOG.debug("    -> out='%s'", out_line)
                out.append(out_line)
    #
    out = "\n".join(out)
    if output_file is None:
        print(out)
    else:
        _LOG.info("Writing '%s'", output_file)
        io_.to_file(output_file, out)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Specify dirs.
    parser.add_argument(
        "--src_dir", action="store", required=True, help="First dir to compare"
    )
    parser.add_argument(
        "--dst_dir", action="store", required=True, help="Second dir to compare"
    )
    # Name dir
    parser.add_argument(
        "--src_dir_name",
        action="store",
        help="A symbolic name for the " "src_dir, e.g., branch_XYZ",
    )
    parser.add_argument(
        "--dst_dir_name",
        action="store",
        help="A symbolic name for the " "dst_dir, e.g., branch_XYZ",
    )
    #
    parser.add_argument(
        "-o",
        "--output_file",
        action="store",
        help="Output file. Don't specify anything for stdout",
    )
    parser.add_argument(
        "--only_diff_content",
        action="store_true",
        help="Show only files that are both present but have different content",
    )
    parser.add_argument(
        "--only_diff_files",
        action="store_true",
        help="Show only files that are not present in both trees",
    )
    parser.add_argument(
        "--skip_comments", action="store_true", help="Do not show comments"
    )
    parser.add_argument(
        "--skip_vim", action="store_true", help="Do not vim commands"
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    src_dir = os.path.abspath(args.src_dir)
    dst_dir = os.path.abspath(args.dst_dir)
    diff_file = _diff(src_dir, dst_dir)
    _parse_diff_output(diff_file, src_dir, dst_dir, args)


if __name__ == "__main__":
    _main(_parse())
