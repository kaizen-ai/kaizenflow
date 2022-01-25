#!/usr/bin/env python

import argparse
import logging
import re

import dev_scripts.replace_text as dscretex
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


def _run() -> None:
    dataframe_to_str = "hpandas.dataframe_to_str"
    df_to_short_str = "hpandas.df_to_short_str"
    df_to_str = "hpandas.df_to_str"
    all_files = dscretex._get_all_files(["."], ["py"])
    # Remove current script from the result.
    all_files.remove(__file__)
    for file_name in all_files:
        file_string = hio.from_file(file_name, encoding=dscretex._ENCODING)
        # Skip non relevant files.
        if not any(
            [
                f"{dataframe_to_str}(" in file_string,
                f"{df_to_short_str}(" in file_string,
            ]
        ):
            continue
        for function_ in [dataframe_to_str, df_to_short_str]:
            # Find all possible patterns for function call.
            function_args = re.findall(f"{function_}\\((.*?)\\)", file_string)
            if function_args:
                for args in function_args:
                    # Apply custom patterns.
                    patterns = [
                        (
                            f"{dataframe_to_str}(df.head())",
                            f'{df_to_str}(df.head(), tag="df")',
                        ),
                        (
                            f"{dataframe_to_str}(df.head(3))",
                            f'{df_to_str}(df.head(3), tag="df")',
                        ),
                        (
                            f"{dataframe_to_str}(reindexed_df.head(3))",
                            f'{df_to_str}(reindexed_df.head(3), tag="df")',
                        ),
                    ]
                    for old_pattern, new_pattern in patterns:
                        if old_pattern in file_string:
                            file_string = file_string.replace(
                                old_pattern, new_pattern
                            )
                            _LOG.info(
                                f"Replace occurrences of `{old_pattern}` in `{file_name}`!"
                            )
                    # Create desired function call.
                    new_args = ""
                    if function_ == dataframe_to_str:
                        # Append generic tag name.
                        # new_args = f'{args}, tag="df"'
                        # TODO(Nikola): Enable tag after merge
                        new_args = args
                    elif function_ == df_to_short_str:
                        # Convert tag positional to keyword argument.
                        new_args_list = args.split(", ")
                        new_args_list.append("print_shape_info=True")
                        new_args_list.append(f"tag={new_args_list[0]}")
                        new_args_list.pop(0)
                        new_args = ", ".join(new_args_list)
                    # Replace occurrences in file.
                    old_regex = f"{function_}({args})"
                    new_regex = f"{df_to_str}({new_args})"
                    file_string = file_string.replace(old_regex, new_regex)
        hio.to_file(file_name, file_string)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run()


if __name__ == "__main__":
    _main(_parse())
