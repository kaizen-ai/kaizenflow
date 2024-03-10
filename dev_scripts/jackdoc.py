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
import subprocess
import urllib.parse
import re

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory
DOCS_DIR = "docs"


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("search_term", help="Input to search in Markdown files (can be a regex)")
    return parser

def _get_git_root():
    try:
        # Run git command to get the root directory of the repository
        git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode().strip()
        return git_root
    except subprocess.CalledProcessError:
        raise RuntimeError("Not a Git repository or Git is not installed.")


def _main(parser):
    args = parser.parse_args()
    search_term = args.search_term

    git_root = _get_git_root()
    docs_path = os.path.join(git_root, DOCS_DIR)

    found_in_files = []
    for root, dirs, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                with open(md_file, 'r') as f:
                    lines = f.readlines()
                    for line_num, line in enumerate(lines, start=1):
                        if re.search(search_term, line):
                            found_in_files.append((md_file, line_num))

    if found_in_files:
        print("Input found in the following Markdown files:")
        for file_path, line_num in found_in_files:
            url = "{}/{}#L{}".format(git_root, file_path[len(git_root) + 1:], line_num)
            print(url)
    else:
        print("Input not found in any Markdown files.")

if __name__ == "__main__":
    _main(_parse())