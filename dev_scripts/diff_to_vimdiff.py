#!/usr/bin/env python

"""
Transform the output of `diff -r --brief dir1 dir2` into a script using vimdiff.

# To clean up the crap in the dirs:
> git status --ignored
> git clean -fdx --dry-run

# Diff dirs:
> diff_to_vimdiff.py --dir1 /Users/saggese/src/commodity_research2/amp --dir2 /Users/saggese/src/commodity_research3/amp
"""

import argparse
import logging
import os
import re

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _diff(dir1: str, dir2: str) -> str:
    """
    Run a diff command between the two dirs and save the output in a file.
    """
    _LOG.info("Comparing dirs %s %s", dir1, dir2)
    dbg.dassert_exists(dir1)
    dbg.dassert_exists(dir2)
    #
    cmd = ""
    cmd += '(cd %s && find %s -name "*" | sort >/tmp/dir1) && ' % (os.path.dirname(dir1), os.path.basename(dir1))
    cmd += '(cd %s && find %s -name "*" | sort >/tmp/dir2)' % (os.path.dirname(dir2), os.path.basename(dir2))
    si.system(cmd, abort_on_error=True)
    #
    cmd = "sdiff /tmp/dir1 /tmp/dir2"
    si.system(cmd, abort_on_error=False, suppress_output=False)
    #
    cmd = "vimdiff /tmp/dir1 /tmp/dir2"
    print("# Diff with:\n> " + cmd)
    #
    dst_file = "/tmp/tmp.diff_to_vimdiff.txt"
    cmd = "diff --brief -r %s %s >%s" % (dir1, dir2, dst_file)
    # We don't abort since diff rc != 0 in case of differences, which is a
    # valid outcome.
    si.system(cmd, abort_on_error=False)
    print("# To see the diff\n> vi %s" % dst_file)
    input_file = dst_file
    #
    return input_file


def _get_symbolic_filepath(dir1, dir2, file_name):
    """
    Transform a path like:
        /Users/saggese/src/commodity_research2/amp/vendors/first_rate/utils.py
    into:
        $DIR1/amp/vendors/first_rate/utils.py
    """
    file_name = file_name.replace(dir1, "$DIR1")
    file_name = file_name.replace(dir2, "$DIR2")
    return file_name


def _parse_diff_output(input_file: str, dir1: str, dir2: str, args):
    """
    Process the output of diff and creates a file with the corresponding.
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
        skip = False
        if line.startswith("Only in "):
            # Only in /data/gp_wd/src/deploy_particle1/: cfile
            m = re.match(r"^Only in (\S+): (\S+)$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
            file_name = "%s/%s" % (m.group(1), m.group(2))
            dbg.dassert_exists(file_name)
            # Comment.
            dir_ = _get_symbolic_filepath(dir1, dir2, m.group(1))
            dirs = dir_.split("/")
            dir_ = dirs[0]
            file_ = os.path.join(
                *dirs[1:],
                _get_symbolic_filepath(dir1, dir2, m.group(2))
            )
            # Determine the direction.
            if "$DIR1" in dir_:
                sign = "<"
            elif "$DIR2" in dir_:
                sign = ">"
            else:
                dfatal("Invalid dir_='%s'" % dir_)
            if args.dir1_name is not None:
                dir_ = dir_.replace("$DIR1", args.dir1_name)
                sign = "<"
            if args.dir2_name is not None:
                dir_ = dir_.replace("$DIR2", args.dir2_name)
            comment = line + "\n"
            comment += "%s: ONLY: %s in '%s'\n" % (sign, file_, dir_)
            # Diff command.
            if args.dir1 in file_name:
                out_line = "vimdiff %s %s" % (
                    file_name,
                    file_name.replace(args.dir1, args.dir2),
                )
            else:
                dbg.dassert_in(args.dir2, file_name)
                out_line = "vimdiff %s %s" % (
                    file_name,
                    file_name.replace(args.dir2, args.dir1),
                )
            # Skip directory.
            if os.path.isdir(file_name):
                _LOG.debug("Skipping dir '%s'", file_name)
                skip = True
            if args.only_different_file_content:
                # We want to see only files with diff content, so skip this.
                _LOG.debug("  -> Skipping line")
                skip = True
        elif line.startswith("Files "):
            # Files
            #   /data/gp_wd/src/deploy_particle1/compustat/fiscal_calendar.py and
            #   /data/gp_wd/src/particle1/compustat/fiscal_calendar.py differ
            m = re.match(r"^Files (\S+) and (\S+) differ$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
            # Check.
            dbg.dassert_exists(m.group(1))
            dbg.dassert_exists(m.group(2))
            # Comment.
            file1 = _get_symbolic_filepath(dir1, dir2, m.group(1))
            file1 = file1.replace("$DIR1/", "")
            file2 = _get_symbolic_filepath(dir1, dir2, m.group(2))
            file2 = file2.replace("$DIR2/", "")
            dbg.dassert_eq(file1, file2)
            sign = "-"
            comment = line + "\n"
            comment += "%s: DIFF: %s\n" % (sign, file1)
            # Diff command.
            out_line = "vimdiff %s %s" % (m.group(1), m.group(2))
            if args.only_different_files:
                _LOG.debug("  -> Skipping line")
                skip = True
        else:
            dbg.dfatal("Invalid line='%s'" % line)
        #
        if not args.skip_comments:
            if comment:
                out.append(prnt.prepend(comment, "#       "))
        if not args.skip_vim:
            if out_line:
                _LOG.debug("    -> out='%s'", out_line)
                if skip:
                    out_line = "# " + out_line
                out.append(out_line)
    #
    out = "\n".join(out)
    if output_file is None:
        print(out)
    else:
        _LOG.info("Writing '%s'", output_file)
        io_.to_file(output_file, out)
        cmd = "chmod +x %s" % output_file
        si.system(cmd)
        # 
        cmd = "./%s" % output_file
        print("Run script with:\n> " + cmd)
        # 
        cmd = "kill -kill $(ps -ef | grep %s | awk '{print $2 }')" % output_file
        print("# To kill the script run:\n> " + cmd)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Specify dirs.
    parser.add_argument(
        "--dir1", action="store", required=True, help="First dir to compare"
    )
    parser.add_argument(
        "--dir2", action="store", required=True, help="Second dir to compare"
    )
    # Name dir.
    parser.add_argument(
        "--dir1_name",
        action="store",
        help="A symbolic name for the dir1, e.g., branch_ABC",
    )
    parser.add_argument(
        "--dir2_name",
        action="store",
        help="A symbolic name for the dir2, e.g., branch_XYZ",
    )
    #
    parser.add_argument(
        "-o",
        "--output_file",
        action="store",
        default="tmp.diff_to_vimdiff.sh",
        help="Output file. Don't specify anything for stdout",
    )
    parser.add_argument(
        "--only_different_file_content",
        action="store_true",
        help="Show only files that are both present but have different content",
    )
    parser.add_argument(
        "--only_different_files",
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
    dir1 = os.path.abspath(args.dir1)
    dir2 = os.path.abspath(args.dir2)
    diff_file = _diff(dir1, dir2)
    _parse_diff_output(diff_file, dir1, dir2, args)


if __name__ == "__main__":
    _main(_parse())
