#!/usr/bin/env python

"""
Convert a txt file into a PDF / HTML using pandoc.

# From scratch with TOC:
> pandoc.py -a pdf --input ...

# For interactive mode:
> pandoc.py -a pdf --no_cleanup_before --no_cleanup --input ...

# Check that can be compiled:
> pandoc.py -a pdf --no_toc --no_open_pdf --input ...

> pandoc.py --input notes/IN_PROGRESS/math.The_hundred_page_ML_book.Burkov.2019.txt -a pdf --no_cleanup --no_cleanup_before --no_run_latex_again --no_open_pdf
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.io_ as io_

# TODO(gp): print_ -> prnt
import helpers.printing as print_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

_EXEC_DIR_NAME = os.path.abspath(os.path.dirname(sys.argv[0]))


def _cleanup_before(prefix):
    _LOG.info("\n" + print_.frame("Clean up before", char1="<", char2=">"))
    cmd = "rm -rf %s*" % prefix
    _ = si.system(cmd, log_level=logging.INFO)


def _remove_empty_lines(curr_path, file_, prefix):
    _LOG.info("\n" + print_.frame("Pre-process", char1="<", char2=">"))
    file1 = file_
    file2 = "%s.no_spaces.txt" % prefix
    cmd = "%s/remove_md_empty_lines.py --input %s --output %s" % (
        curr_path,
        file1,
        file2,
    )
    _ = si.system(cmd, log_level=logging.INFO)
    file_ = file2
    return file_


def _run_pandoc(args, curr_path, file_, prefix):
    # --filter /Users/$USER/src/github/pandocfilters/examples/tikz.py \
    # -F /Users/$USER/src/github/pandocfilters/examples/lilypond.py \
    _LOG.info("\n" + print_.frame("Pandoc", char1="<", char2=">"))
    file1 = file_
    # --filter pandoc-imagine
    cmd = [
        "pandoc %s" % file1,
        "-V geometry:margin=1in",
        "-f markdown",
        "--number-sections",
        # - To change the highlight style
        # https://github.com/jgm/skylighting
        "--highlight-style=tango",
        "-s",
    ]
    if args.action == "pdf":
        cmd.append("-t latex")
        template = "%s/pandoc.latex" % curr_path
        dbg.dassert_exists(template)
        cmd.append("--template %s" % template)
        file2 = "%s.tex" % prefix
    elif args.action == "html":
        cmd.append("-t html")
        cmd.append("--metadata pagetitle='%s'" % os.path.basename(file_))
        file2 = "%s.html" % prefix
    else:
        raise ValueError("Invalid action '%s'" % args.action)
    cmd.append("-o %s" % file2)
    if not args.no_toc:
        cmd.append("--toc")
        cmd.append("--toc-depth 2")
    else:
        args.no_run_latex_again = True
    # Doesn't work
    # -f markdown+raw_tex
    cmd = " ".join(cmd)
    _ = si.system(cmd, suppress_output=False, log_level=logging.INFO)
    file_ = file2
    if args.action == "pdf":
        #
        # Run latex.
        #
        _LOG.info("\n" + print_.frame("Latex", char1="<", char2=">"))
        # pdflatex needs to run in the same dir of latex_abbrevs.sty so we
        # cd to that dir and save the output in the same dir of the input.
        dbg.dassert_exists(_EXEC_DIR_NAME + "/latex_abbrevs.sty")
        cmd = "cd %s; " % _EXEC_DIR_NAME
        cmd += (
            "pdflatex"
            + " -interaction=nonstopmode"
            + " -halt-on-error"
            + " -shell-escape"
            + " -output-directory %s" % os.path.dirname(file_)
            + " %s" % file_
        )

        def _run_latex():
            rc, txt = si.system_to_string(
                cmd, abort_on_error=False, log_level=logging.INFO
            )
            log_file = file_ + ".latex1.log"
            io_.to_file(log_file, txt)
            if rc != 0:
                txt = txt.split("\n")
                for i in range(len(txt)):
                    if txt[i].startswith("!"):
                        break
                txt = [
                    txt[i] for i in range(max(i - 10, 0), min(i + 10, len(txt)))
                ]
                txt = "\n".join(txt)
                _LOG.error(txt)
                _LOG.error("Log is in %s", log_file)
                _LOG.error("\n" + print_.frame("cmd is:\n> %s" % cmd))
                raise RuntimeError("Latex failed")

        _run_latex()
        # Run latex again.
        _LOG.info("\n" + print_.frame("Latex again", char1="<", char2=">"))
        if not args.no_run_latex_again:
            _run_latex()
        else:
            _LOG.warning("Skipping: run latex again")


def _copy_to_output(args, prefix):
    src_pdf_file = "%s.pdf" % prefix
    if args.output is not None:
        dst_pdf_file = args.output
    else:
        dst_pdf_file = "%s.%s.pdf" % (prefix, os.path.basename(args.input))
    cmd = "cp -a %s %s" % (src_pdf_file, dst_pdf_file)
    _ = si.system(cmd, log_level=logging.INFO)
    pdf_file = dst_pdf_file
    return pdf_file


def _open_pdf(args, pdf_file):
    _LOG.info("\n" + print_.frame("Open PDF", char1="<", char2=">"))
    # open $pdfFile
    cmd = (
        """
/usr/bin/osascript << EOF
set theFile to POSIX file "%s" as alias
tell application "Skim"
activate
set theDocs to get documents whose path is (get POSIX path of theFile)
if (count of theDocs) > 0 then revert theDocs
open theFile
end tell
EOF
            """
        % pdf_file
    )
    _ = si.system(cmd, log_level=logging.INFO)
    cmd = "open -a Skim %s" % pdf_file
    _ = si.system(cmd, log_level=logging.INFO)


def _copy_to_gdrive(args, pdf_file):
    _LOG.info("\n" + print_.frame("Copy to gdrive", char1="<", char2=">"))
    if args.gdrive_dir is not None:
        gdrive_dir = args.gdrive_dir
    else:
        gdrive_dir = "/Users/saggese/GoogleDrive/pdf_notes"
    dbg.dassert_dir_exists(gdrive_dir)
    dst_file = (
        gdrive_dir + "/" + os.path.basename(args.input).replace(".txt", ".pdf")
    )
    cmd = "cp -a %s %s" % (pdf_file, dst_file)
    _ = si.system(cmd, log_level=logging.INFO)


def _cleanup_after(prefix):
    _LOG.info("\n" + print_.frame("Clean up", char1="<", char2=">"))
    cmd = "rm -rf %s*" % prefix
    _ = si.system(cmd, log_level=logging.INFO)


# ##############################################################################


def _pandoc(args, cmd_line):
    #
    _LOG.info("cmd=%s", cmd_line)
    _LOG.info("actions=%s", " ".join(args.action))
    #
    curr_path = os.path.abspath(os.path.dirname(sys.argv[0]))
    _LOG.debug("curr_path=%s", curr_path)
    #
    file_ = args.input
    dbg.dassert_exists(file_)
    prefix = args.tmp_dir + "/tmp.pandoc"
    prefix = os.path.abspath(prefix)
    #
    if not args.no_cleanup_before:
        _cleanup_before(prefix)
    else:
        _LOG.warning("Skipping: clean up before")
    #
    if not args.no_remove_empty_lines:
        file_ = _remove_empty_lines(curr_path, file_, prefix)
    else:
        _LOG.warning("skipping remove empty lines")
    #
    if not args.no_run_pandoc:
        _run_pandoc(args, curr_path, file_, prefix)
    else:
        _LOG.warning("Skipping: run pandoc")
    #
    pdf_file = _copy_to_output(args, prefix)
    #
    if not args.no_gdrive:
        _copy_to_gdrive(args, pdf_file)
    else:
        _LOG.warning("Skipping: copy to gdrive")
    #
    if not args.no_open_pdf:
        _open_pdf(args, pdf_file)
    else:
        _LOG.warning("Skipping: open pdf")
    #
    if not args.no_cleanup:
        _cleanup_after(prefix)
    else:
        _LOG.warning("Skipping: clean up")
    #
    _LOG.info("\n" + print_.frame("SUCCESS"))


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-a", "--action", required=True, choices=["pdf", "html"], action="store"
    )
    parser.add_argument("--input", action="store", type=str, required=True)
    parser.add_argument(
        "--output", action="store", type=str, default=None, help="Output file"
    )
    parser.add_argument(
        "--tmp_dir",
        action="store",
        type=str,
        default=".",
        help="Directory where to save artifacts",
    )
    parser.add_argument("--no_cleanup_before", action="store_true", default=False)
    parser.add_argument(
        "--no_remove_empty_lines", action="store_true", default=False
    )
    parser.add_argument("--no_run_pandoc", action="store_true", default=False)
    parser.add_argument("--no_toc", action="store_true", default=False)
    parser.add_argument(
        "--no_run_latex_again", action="store_true", default=False
    )
    parser.add_argument("--no_gdrive", action="store_true", default=False)
    parser.add_argument(
        "--gdrive_dir",
        action="store_true",
        default=None,
        help="Directory where to save the output",
    )
    parser.add_argument("--no_open_pdf", action="store_true", default=False)
    parser.add_argument("--no_cleanup", action="store_true", default=False)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    cmd_line = " ".join(map(str, sys.argv))
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    _pandoc(args, cmd_line)


if __name__ == "__main__":
    _main(_parse())
