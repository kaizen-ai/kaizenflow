#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.
Check dev_scripts/linter.py to see an example of a script using this
template.
Import as:
import dev_scripts.script_skeleton as dscscske
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hpickle as hpickle

# import helpers.hsystem as hsystem
import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input_file", action="store", required=True, help="")
    parser.add_argument("--output_file", action="store", required=True, help="")
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    input_obj = hpickle.from_pickle(args.input_file)
    hdbg.dassert_isinstance(input_obj, dict)
    hdbg.dassert_eq(len(input_obj), 2)
    hdbg.dassert_in("config", input_obj.keys())
    config = input_obj["config"]
    hdbg.dassert_in("df", input_obj.keys())
    df = input_obj["df"]
    #
    output_df = osipeopt.optimize(config, df)
    #
    hpickle.to_pickle(output_df, args.output_file)


if __name__ == "__main__":
    _main(_parse())
