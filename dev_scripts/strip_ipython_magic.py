#!/usr/bin/env python

import argparse
import logging
import re

import helpers.dbg as dbg
import helpers.system_interaction as hsi

_log = logging


# These functions were provided by https://github.com/rgerkin
# https://github.com/jupyter/nbconvert/issues/503
def strip_line_magic(line):
    _log.debug("line=%s", line)
    # get_ipython().run_cell_magic(
    #   u'time', u'',
    #   u"all_earnings['month_year'] =
    #       all_earnings['timestamp_ET'].apply(\ n    lambda x:
    #       datetime.strptime('{}-{}-01'.format(x.year, x.month),
    #       '%Y-%m-%d')\n)")
    matches = re.findall(r"run_cell_magic\((.*)\)", line)
    if matches:
        _log.debug("-> matches=%s", matches)
        print()
        print(("\n".join(matches)))
        # This line contains the pattern.
        matches[0]
        stripped = "\n".join(matches[1:])
        #_log.debug("stripped=%s", "\n".join(stripped))

        print("stripped")
        print()
        print(stripped)
        assert 0
    else:
        stripped = line
    return stripped


def strip_magic(file_in, file_out):
    if file_out is None:
        file_out = file_in
    _log.debug("file_in='%s' -> file_out='%s'", file_in, file_out)
    # Read file.
    with open(file_in) as f:
        code = f.read()
    code = code.split('\n')
    # Strip out magic.
    n_code = []
    for cl in code:
        n_code.append(strip_line_magic(cl))
    # Write.
    with open(file_out, 'w') as fo:
        for item in n_code:
            fo.write("%s\n" % item)


def _main(args):
    dbg.init_logger2(args.log_level)
    for f in args.files:
        _log.info("Converting %s", f)
        dbg.dassert(f.endswith(".ipynb"), msg="Invalid file=%s" % f)
        cmd = "jupyter nbconvert %s --to python" % f
        #--template=dev_scripts/ipy_to_py.tpl" % f
        hsi.system(cmd)
        if True:
            file_out = f.replace(".ipynb", ".py")
            #
            file_in = file_out[:]
            file_out = file_out.replace(".py", ".stripped.py")
            strip_magic(file_in, file_out=file_out)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--files', type=str, action='append', default=[])
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()
    _main(args)


if __name__ == "__main__":
    _parse()
