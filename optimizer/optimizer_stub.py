#!/usr/bin/env python

"""
Run the optimizer using an input file and save the result into an output file.

E.g., to run optimizer using `input.pkl` as an input file and to save the output
to `output.pkl` do:
> optimizer_stub.py --input_file input.pkl --output_file output.pkl

Import as:
import dev_scripts.script_skeleton as dscscske
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hpickle as hpickle

import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input_file", action="store", required=True, help="file with the input data for optimizer.")
    parser.add_argument("--output_file", action="store", required=True, help="file with the optimizer output data.")
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Read the input data.
    input_obj = hpickle.from_pickle(args.input_file)
    hdbg.dassert_isinstance(input_obj, dict)
    hdbg.dassert_eq(len(input_obj), 2)
    hdbg.dassert_in("config", input_obj.keys())
    config = input_obj["config"]
    hdbg.dassert_in("df", input_obj.keys())
    df = input_obj["df"]
    # Run the optimizer.
    output_df = osipeopt.optimize(config, df)
    # Save the output.
    hpickle.to_pickle(output_df, args.output_file)


if __name__ == "__main__":
    _main(_parse())
