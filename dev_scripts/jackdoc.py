#!/usr/bin/env python

"""
jackdoc: A tool to search for input in Markdown files and provide links to the files where the input was found.

Example usage:
jackdoc "search_term"

Import as:

import dev_scripts.jackdoc as jackdoc
"""

import argparse
import logging
import os

# import helpers.hdbg as hdbg
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

# Define the root directory where Markdown files are located
DOCS_DIR = "/docs"


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("search_term", help="Input to search in Markdown files")
    return parser

def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    search_term = args.search_term

    found_in_files = []
    for root, dirs, files in os.walk(DOCS_DIR):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                # Use jackmd tool to search for the search term in the current Markdown file
                result = hsystem.system_to_string(f"jackmd '{search_term}' '{md_file}'")
                # If the search term is found in the file, append the file path to the list
                if result:
                    found_in_files.append(md_file)

    if found_in_files:
        print("Input found in the following Markdown files:")
        for file_path in found_in_files:
            print(f"- {file_path}")
    else:
        print("Input not found in any Markdown files.")

if __name__ == "__main__":
    _main(_parse())