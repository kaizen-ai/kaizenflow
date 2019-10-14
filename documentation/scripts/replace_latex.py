#!/usr/bin/env python

"""
* Replace only:
> scripts/replace_latex.py -a replace --file notes/IN_PROGRESS/finance.portfolio_theory.txt

* Replace and check:
> scripts/replace_latex.py -a pandoc_before -a replace -a pandoc_after --file notes/IN_PROGRESS/finance.portfolio_theory.txt
"""

import argparse
import logging
import re

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# ##############################################################################


def _standard_cleanup(in_file, aggressive):
    # - Always use "you" instead of "one"
    # - Try to make the wording as terse as possible
    # - Always use $\cdot$
    dbg.dassert_exists(in_file)
    txt = io_.from_file(in_file)
    out = []
    for l in txt:
        for s, d in [
            ("gaussian", "Gaussian"),
            ("iid", "IID"),
            ("doesn't", "does not"),
            ("can't", "cannot"),
            ("it's", "it is"),
            ("'s", " is"),
            ("they're", "they are"),
            ("isn't", "is not"),
            ("aren't", "are not"),
            ("wrt", "with respect to"),
            ("we", "you"),
            ("one", "you"),
            # \bbR -> \R ?
            # see https://oeis.org/wiki/List_of_LaTeX_mathematical_symbols
        ]:
            # l = l.replace(s, d)
            l = re.sub("\\b" + s + "\\b", d, l)
            l = re.sub("\\b" + s.capitalize() + "\\b", d.capitalize(), l)
        for re1, re2 in [
            # Replace "iff" with "$\iff$" unless it's in a word or it's
            # alread $\iff$.
            (r"\b(?<!\\)iff\b", r"$\\iff$"),
            # (nasdaq -> NASDAQ)
            # \textit{Answer}
            (r"^\\textit{(.*?)}", r"- ___\1___"),
            # (\textit{} -> _ _)
            (r"\\textit{(.*?)}", r"_\1_"),
        ]:
            l = re.sub(re1, re2, l)
        # This can't be automatic, but needs to be verified by hand.
        if aggressive:
            for s, d in [(r"\\d=", r"\\dd=")]:
                l = re.sub("\\b" + s + "\\b", d, l)
                l = re.sub("\\b" + s.capitalize() + "\\b", d.capitalize(), l)

            def _repl_func(m):
                return m.group(1) + m.group(2).upper() + m.group(3)

            l = re.sub("^(\s*- )(\S)(.*)", _repl_func, l)
        # Remove spaces at the end of the line.
        l = re.sub("\s+$", "", l)
        out.append(l)
    out = "\n".join(out)
    io_.to_file(in_file, out)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    dbg.dassert_exists(args.file)
    actions = args.action
    if not isinstance(actions, list):
        actions = list(actions)
    if "checkout" in actions:
        cmd = "git checkout -- %s" % args.file
        _ = si.system(cmd)
    if "pandoc_before" in actions:
        cmd = "pandoc.py -a pdf --no_toc --no_open_pdf --input %s" % args.file
        _ = si.system(cmd)
    if "replace" in actions:
        _standard_cleanup(args.file, args.aggressive)
    if "pandoc_after" in actions:
        cmd = "pandoc.py -a pdf --no_toc --no_open_pdf --input %s" % args.file
        _ = si.system(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-a",
        "--action",
        required=True,
        choices=["checkout", "pandoc_before", "pandoc_after", "replace"],
        action="append",
    )
    parser.add_argument("--file", action="store", type=str, required=True)
    parser.add_argument("--aggressive", action="store_true")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    _main(parser)
