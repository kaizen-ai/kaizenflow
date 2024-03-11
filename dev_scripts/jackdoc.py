#!/usr/bin/env python

"""
jackdoc: A tool to search for input in Markdown files and provide links to the files where the input was found.

Example usage:
jackdoc "search_term" [--skip-toc] [--sections-only] [--subdir <subdirectory>]

Import as:

import dev_scripts.jackdoc as jackdoc
"""

import argparse
import logging
import os
import subprocess
import re
import helpers.hio as hio

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory
DOCS_DIR = "docs"


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("search_term", help="Regex pattern to search in Markdown files")
    parser.add_argument("--skip-toc", action="store_true", help="Skip results in the table of contents (TOC)")
    parser.add_argument("--sections-only", action="store_true", help="Search only in sections (lines starting with '#')")
    parser.add_argument("--subdir", help="Subdirectory to search within")
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
    skip_toc = args.skip_toc
    sections_only = args.sections_only
    subdir = args.subdir

    git_root = _get_git_root()
    docs_path = os.path.join(git_root, DOCS_DIR)

    if subdir:
        docs_path = os.path.join(docs_path, subdir)

    found_in_files = []

    for root, dirs, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                # Read the content of the Markdown file
                content = hio.from_file(md_file)
                lines = content.split('\n')
                toc_flag = False  # Flag to indicate if we are within the TOC
                for line_num, line in enumerate(lines, start=1):
                    if "<!-- toc -->" in line:
                        toc_flag = True
                        if skip_toc:
                            break  # Skip searching if --skip-toc is enabled
                        continue
                    elif "<!-- tocstop -->" in line:
                        toc_flag = False
                        continue

                    if toc_flag and skip_toc:
                        continue  # Skip lines within the TOC if --skip-toc is enabled

                    if sections_only and not re.match(r'^#+\s', line):
                        continue  # Skip lines if --sections-only is enabled and not a section heading

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