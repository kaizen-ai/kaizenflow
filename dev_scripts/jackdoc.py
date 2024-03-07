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
import urllib.parse

_LOG = logging.getLogger(__name__)

# Define the root directory where Markdown files are located
DOCS_DIR = "/src/sorrentum1/docs"


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
                with open(md_file, 'r') as f:
                    lines = f.readlines()
                    for line_num, line in enumerate(lines, start=1):
                        if search_term in line:
                            found_in_files.append((md_file, line_num))

    if found_in_files:
        print("Input found in the following Markdown files:")
        for file_path, line_num in found_in_files:
            url = f"https://github.com/sorrentum/sorrentum/blob/master/{file_path[len(DOCS_DIR) + 1:]}?plain=1#L{line_num}"
            print(url)
    else:
        print("Input not found in any Markdown files.")

if __name__ == "__main__":
    _main(_parse())