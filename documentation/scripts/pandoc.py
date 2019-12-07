#!/usr/bin/env python

"""
Convert a txt file into a PDF / HTML using pandoc.

# From scratch with TOC:
> pandoc.py -a pdf --input ...

# For interactive mode:
> pandoc.py -a pdf --no_cleanup_before --no_cleanup --input ...

# Check that can be compiled:
> pandoc.py -a pdf --no_toc --no_open_pdf --input ...

> pandoc.py --input notes/IN_PROGRESS/math.The_hundred_page_ML_book.Burkov.2019.txt -a pdf --no_cleanup --no_cleanup_before --no_run_latex_again --no_open
"""

# TODO(gp):
#  - clean up the file_ file_out logic and make it a pipeline
#  - factor out the logic from linter.py about actions and use it everywhere


import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

_EXEC_DIR_NAME = os.path.abspath(os.path.dirname(sys.argv[0]))

# #############################################################################

_script = None


def _system(cmd, log_level=logging.INFO, **kwargs):
    if _script is not None:
        _script.append(cmd)
    rc = si.system(cmd, log_level=log_level, **kwargs)
    return rc


def _system_to_string(cmd, log_level=logging.INFO, **kwargs):
    if _script is not None:
        _script.append(cmd)
    rc, txt = si.system_to_string(cmd, log_level=log_level, **kwargs)
    return rc, txt


# #############################################################################


def _cleanup_before(prefix):
    _LOG.info("\n%s", prnt.frame("Clean up before", char1="<", char2=">"))
    cmd = "rm -rf %s*" % prefix
    _ = _system(cmd)


def _convert_txt_to_pandoc(curr_path, file_, prefix):
    _LOG.info("\n%s", prnt.frame("Pre-process markdown", char1="<", char2=">"))
    file1 = file_
    file2 = "%s.no_spaces.txt" % prefix
    cmd = "%s/convert_txt_to_pandoc.py --input %s --output %s" % (
        curr_path,
        file1,
        file2,
    )
    _ = _system(cmd)
    file_ = file2
    return file_


_COMMON_PANDOC_OPTS = [
    "-V geometry:margin=1in",
    "-f markdown",
    "--number-sections",
    # - To change the highlight style
    # https://github.com/jgm/skylighting
    "--highlight-style=tango",
    "-s",
]
# --filter /Users/$USER/src/github/pandocfilters/examples/tikz.py \
# -F /Users/$USER/src/github/pandocfilters/examples/lilypond.py \
# --filter pandoc-imagine


def _run_pandoc_to_pdf(args, curr_path, file_, prefix):
    _LOG.info("\n%s", prnt.frame("Pandoc to pdf", char1="<", char2=">"))
    #
    file1 = file_
    cmd = []
    cmd.append("pandoc %s" % file1)
    cmd.extend(_COMMON_PANDOC_OPTS[:])
    #
    cmd.append("-t latex")
    template = "%s/pandoc.latex" % curr_path
    dbg.dassert_exists(template)
    cmd.append("--template %s" % template)
    #
    file2 = "%s.tex" % prefix
    cmd.append("-o %s" % file2)
    if not args.no_toc:
        cmd.append("--toc")
        cmd.append("--toc-depth 2")
    else:
        args.no_run_latex_again = True
    # Doesn't work
    # -f markdown+raw_tex
    cmd = " ".join(cmd)
    _ = _system(cmd, suppress_output=False)
    file_ = file2
    #
    # Run latex.
    #
    _LOG.info("\n%s", prnt.frame("Latex", char1="<", char2=">"))
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
        rc, txt = _system_to_string(cmd, abort_on_error=False)
        log_file = file_ + ".latex1.log"
        io_.to_file(log_file, txt)
        if rc != 0:
            txt = txt.split("\n")
            for i in range(len(txt)):
                if txt[i].startswith("!"):
                    break
            txt = [txt[i] for i in range(max(i - 10, 0), min(i + 10, len(txt)))]
            txt = "\n".join(txt)
            _LOG.error(txt)
            _LOG.error("Log is in %s", log_file)
            _LOG.error("\n%s", prnt.frame("cmd is:\n> %s" % cmd))
            raise RuntimeError("Latex failed")

    _run_latex()
    # Run latex again.
    _LOG.info("\n%s", prnt.frame("Latex again", char1="<", char2=">"))
    if not args.no_run_latex_again:
        _run_latex()
    else:
        _LOG.warning("Skipping: run latex again")
    #
    file_out = file_.replace(".tex", ".pdf")
    _LOG.debug("file_out=%s", file_out)
    dbg.dassert_exists(file_out)
    return file_out


def _run_pandoc_to_html(args, file_in, prefix):
    _LOG.info("\n%s", prnt.frame("Pandoc to html", char1="<", char2=">"))
    #
    cmd = []
    cmd.append("pandoc %s" % file_in)
    cmd.extend(_COMMON_PANDOC_OPTS[:])
    cmd.append("-t html")
    cmd.append("--metadata pagetitle='%s'" % os.path.basename(file_in))
    #
    file2 = "%s.html" % prefix
    cmd.append("-o %s" % file2)
    if not args.no_toc:
        cmd.append("--toc")
        cmd.append("--toc-depth 2")
    cmd = " ".join(cmd)
    _ = _system(cmd, suppress_output=False)
    #
    file_out = os.path.abspath(file2.replace(".tex", ".html"))
    _LOG.debug("file_out=%s", file_out)
    dbg.dassert_exists(file_out)
    return file_out


def _copy_to_output(args, file_in, prefix):
    if args.output is not None:
        _LOG.debug("Using file_out from command line")
        file_out = args.output
    else:
        _LOG.debug("Leaving file_out in the tmp dir")
        file_out = "%s.%s.%s" % (
            prefix,
            os.path.basename(args.input),
            args.action,
        )
    _LOG.debug("file_out=%s", file_out)
    cmd = r"\cp -af %s %s" % (file_in, file_out)
    _ = _system(cmd)
    return file_out


def _copy_to_gdrive(args, file_name, ext):
    _LOG.info("\n%s", prnt.frame("Copy to gdrive", char1="<", char2=">"))
    dbg.dassert(not ext.startswith("."), "Invalid file_name='%s'", file_name)
    if args.gdrive_dir is not None:
        gdrive_dir = args.gdrive_dir
    else:
        gdrive_dir = "/Users/saggese/GoogleDrive/pdf_notes"
    # Copy.
    dbg.dassert_dir_exists(gdrive_dir)
    _LOG.debug("gdrive_dir=%s", gdrive_dir)
    basename = os.path.basename(args.input).replace(".txt", "." + ext)
    _LOG.debug("basename=%s", basename)
    dst_file = os.path.join(gdrive_dir, basename)
    cmd = r"\cp -af %s %s" % (file_name, dst_file)
    _ = _system(cmd)
    _LOG.debug("Saved file='%s' to gdrive", dst_file)


def _open_pdf(file_name):
    _LOG.info("\n%s", prnt.frame("Open PDF", char1="<", char2=">"))
    dbg.dassert_exists(file_name)
    dbg.dassert_file_extension(file_name, "pdf")
    #
    _LOG.debug("Opening file='%s'", file_name)
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
        % file_name
    )
    _ = _system(cmd)
    cmd = "open -a Skim %s" % file_name
    _ = _system(cmd)


def _open_html(file_name):
    _LOG.info("\n%s", prnt.frame("Open HTML", char1="<", char2=">"))
    dbg.dassert_exists(file_name)
    dbg.dassert_file_extension(file_name, "html")
    #
    _LOG.debug("Opening file='%s'", file_name)
    cmd = "open %s" % file_name
    _ = _system(cmd)


def _cleanup_after(prefix):
    _LOG.info("\n%s", prnt.frame("Clean up", char1="<", char2=">"))
    cmd = "rm -rf %s*" % prefix
    _ = _system(cmd)


# ##############################################################################


def _pandoc(args, cmd_line):
    #
    _LOG.info("cmd=%s", cmd_line)
    _LOG.info("actions=%s", " ".join(args.action))
    #
    curr_path = os.path.abspath(os.path.dirname(sys.argv[0]))
    _LOG.debug("curr_path=%s", curr_path)
    #
    if args.script:
        _LOG.info("Logging the actions into a script")
        global _script
        _script = ["#/bin/bash -xe"]
    #
    file_ = args.input
    dbg.dassert_exists(file_)
    prefix = args.tmp_dir + "/tmp.pandoc"
    prefix = os.path.abspath(prefix)
    #
    # Clean up before.
    #
    if not args.no_cleanup_before:
        _cleanup_before(prefix)
    else:
        _LOG.warning("Skipping: clean up before")
    #
    # Pre-process markdown.
    #
    if not args.no_convert_txt_to_pandoc:
        file_ = _convert_txt_to_pandoc(curr_path, file_, prefix)
    else:
        _LOG.warning("Skipping: pre-process markdown")
    #
    # Run pandoc.
    #
    if not args.no_run_pandoc:
        if args.action == "pdf":
            file_out = _run_pandoc_to_pdf(args, curr_path, file_, prefix)
        elif args.action == "html":
            file_out = _run_pandoc_to_html(args, file_, prefix)
        else:
            raise ValueError("Invalid action='%s'" % args.action)
    else:
        _LOG.warning("Skipping: run pandoc")
    #
    file_in = file_out
    file_final = _copy_to_output(args, file_in, prefix)
    #
    # Copy to gdrive.
    #
    if not args.no_gdrive:
        ext = args.action
        _copy_to_gdrive(args, file_final, ext)
    else:
        _LOG.warning("Skipping: copy to gdrive")
    #
    # Open file.
    #
    if not args.no_open:
        os_name = si.get_os_name()
        if os_name == "Darwin":
            if args.action == "pdf":
                _open_pdf(file_final)
            elif args.action == "html":
                _open_html(file_final)
            else:
                raise ValueError("Invalid action='%s'" % args.action)
        else:
            # TODO(gp): Extend this.
            _LOG.warning("Can't open file on %s", os_name)
    else:
        _LOG.warning("Skipping: open")
    #
    # Clean up.
    #
    if not args.no_cleanup:
        _cleanup_after(prefix)
    else:
        _LOG.warning("Skipping: clean up")
    #
    # Save script, if needed.
    #
    if args.script:
        txt = "\n".join(_script)
        io_.to_file(args.script, txt)
        _LOG.info("Saved script into '%s'", args.script)
    #
    _LOG.info("\n%s", prnt.frame("SUCCESS"))


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
    parser.add_argument(
        "--script", action="store", help="Bash script to generate"
    )
    # Control phases.
    parser.add_argument("--no_cleanup_before", action="store_true", default=False)
    parser.add_argument("--no_convert_txt_to_pandoc", action="store_true", default=False)
    parser.add_argument("--no_run_pandoc", action="store_true", default=False)
    parser.add_argument("--no_toc", action="store_true", default=False)
    parser.add_argument(
        "--no_run_latex_again", action="store_true", default=False
    )
    parser.add_argument("--no_gdrive", action="store_true", default=False)
    parser.add_argument(
        "--gdrive_dir",
        action="store",
        default=None,
        help="Directory where to save the output",
    )
    parser.add_argument("--no_open", action="store_true", default=False)
    parser.add_argument("--no_cleanup", action="store_true", default=False)
    #
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    cmd_line = " ".join(map(str, sys.argv))
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _pandoc(args, cmd_line)


if __name__ == "__main__":
    _main(_parse())
