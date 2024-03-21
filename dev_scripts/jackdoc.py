#!/usr/bin/env python

"""
jackdoc: Locate input from md files in docs dir and generate corresponding file links.

Example usage:
jackdoc "search_term" [--skip-toc] [--sections-only] [--subdir <subdirectory>]

"""

import argparse
import logging
import os
import re

import helpers.hgit as hgit
import helpers.hio as hio

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory.
DOCS_DIR = "docs"


def parse_arguments():
    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "search_term", help="Regex pattern to search in Markdown files"
    )
    parser.add_argument(
        "--skip-toc",
        action="store_true",
        help="Skip results in the table of contents (TOC)",
    )
    parser.add_argument(
        "--sections-only",
        action="store_true",
        help="Search only in sections (lines starting with '#')",
    )
    parser.add_argument("--subdir", help="Subdirectory to search within")
    return parser.parse_args()


def remove_toc(content):
    # Remove table of contents from Markdown content.
    toc_pattern = r"<!--\s*toc\s*-->(.*?)<!--\s*tocstop\s*-->"
    return re.sub(toc_pattern, "", content, flags=re.DOTALL)


def search_in_markdown_files(
    git_root, search_term, skip_toc=False, sections_only=False, subdir=None
):
    found_in_files = []
    docs_path = (
        os.path.join(git_root, DOCS_DIR, subdir)
        if subdir
        else os.path.join(git_root, DOCS_DIR)
    )

    for root, _, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                content = hio.from_file(md_file)
                content = remove_toc(content) if skip_toc else content
                lines = content.split("\n")
                for line_num, line in enumerate(lines, start=1):
                    if sections_only and not line.startswith("#"):
                        continue
                    if re.search(search_term, line):
                        found_in_files.append((md_file, line_num))

    return found_in_files


def main():
    args = parse_arguments()
    git_root = hgit.get_client_root(super_module=True)
    found_in_files = search_in_markdown_files(
        git_root, args.search_term, args.skip_toc, args.sections_only, args.subdir
    )

    if found_in_files:
        _LOG.info("Input found in the following Markdown files:")
        for file_path, line_num in found_in_files:
            relative_path = os.path.relpath(file_path, git_root)
            url = f"{git_root}/{relative_path}#L{line_num}"
            _LOG.info(url)
    else:
        _LOG.info("Input not found in any Markdown files.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()