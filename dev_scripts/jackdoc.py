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
import helpers.hgit as hgit

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory
DOCS_DIR = "docs"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("search_term", help="Regex pattern to search in Markdown files")
    parser.add_argument("--skip-toc", action="store_true", help="Skip results in the table of contents (TOC)")
    parser.add_argument("--sections-only", action="store_true", help="Search only in sections (lines starting with '#')")
    parser.add_argument("--subdir", help="Subdirectory to search within")
    return parser.parse_args()


def remove_toc(content):
    # Remove everything between <!-- toc --> and <!-- tocstop -->
    toc_pattern = r"<!--\s*toc\s*-->(.*?)<!--\s*tocstop\s*-->"
    return re.sub(toc_pattern, "", content, flags=re.DOTALL)


def search_in_markdown_files(client_root, search_term, skip_toc=False, sections_only=False, subdir=None):
    found_in_files = []
    docs_path = os.path.join(client_root, DOCS_DIR, subdir) if subdir else os.path.join(client_root, DOCS_DIR)

    for root, _, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                # Read the content of the Markdown file
                content = hio.from_file(md_file)
                # If --skip-toc is enabled, remove TOC from the content
                content = remove_toc(content) if skip_toc else content
                lines = content.split('\n')
                for line_num, line in enumerate(lines, start=1):
                    if sections_only and not line.startswith('#'):
                        continue  # Skip lines if --sections-only is enabled and not a section heading

                    if re.search(search_term, line):
                        found_in_files.append((md_file, line_num))

    return found_in_files


def main():
    args = parse_arguments()
    client_root = hgit.get_client_root()
    found_in_files = search_in_markdown_files(
        client_root, args.search_term, args.skip_toc, args.sections_only, args.subdir
    )

    if found_in_files:
        print("Input found in the following Markdown files:")
        for file_path, line_num in found_in_files:
            relative_path = os.path.relpath(file_path, client_root)
            url = f"{client_root}/{relative_path}#L{line_num}"
            print(url)
    else:
        print("Input not found in any Markdown files.")


if __name__ == "__main__":
    main()