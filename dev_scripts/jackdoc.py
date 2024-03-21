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
import subprocess
from typing import List, Optional, Tuple

import helpers.hgit as hgit
import helpers.hio as hio

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory.
DOCS_DIR: str = "docs"


def _parse() -> argparse.ArgumentParser:
    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "search_term", help="Term to search in Markdown files"
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
    return parser


def get_github_info() -> Tuple[str, str]:
    try:
        remote_url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
        match = re.match(r'^https://github.com/(.*)/(.*)\.git$', remote_url)
        if match:
            return match.group(1), match.group(2)
    except (subprocess.CalledProcessError, IndexError) as exc:
        raise RuntimeError('Unable to get remote URL from git configuration') from exc
    raise ValueError("Not a GitHub repository")


def _remove_toc(content: str) -> str:
    # Remove table of contents from Markdown content.
    toc_pattern: str = r"<!--\s*toc\s*-->(.*?)<!--\s*tocstop\s*-->"
    return re.sub(toc_pattern, "", content, flags=re.DOTALL)


def _search_in_markdown_files(
    git_root: str,
    search_term: str,
    skip_toc: bool = False,
    sections_only: bool = False,
    subdir: Optional[str] = None
) -> List[Tuple[str, int]]:
    found_in_files: List[Tuple[str, int]] = []
    docs_path: str = (
        os.path.join(git_root, DOCS_DIR, subdir)
        if subdir
        else os.path.join(git_root, DOCS_DIR)
    )

    for root, _, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file: str = os.path.join(root, file)
                content: str = hio.from_file(md_file)
                content: str = _remove_toc(content) if skip_toc else content
                lines: List[str] = content.split("\n")
                for line_num, line in enumerate(lines, start=1):
                    if sections_only and not line.startswith("#"):
                        continue
                    if re.search(search_term, line):
                        found_in_files.append((md_file, line_num))
    return found_in_files


def _main(parser: argparse.ArgumentParser) -> None:
    args: argparse.Namespace = parser.parse_args()
    git_root: str = hgit.get_client_root(super_module=True)
    username, repo = get_github_info()
    found_in_files: List[Tuple[str, int]] = _search_in_markdown_files(
        git_root, args.search_term, args.skip_toc, args.sections_only, args.subdir
    )

    if found_in_files:
        _LOG.info("Input found in the following Markdown files:")
        for file_path, line_num in found_in_files:
            relative_path: str = os.path.relpath(file_path, git_root)
            # Constructing the GitHub URL based on --sections-only flag.
            if args.sections_only:
                # Extracting section name using regular expression.
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                section_match = re.search(r'^#+\s*(.*)$', content.splitlines()[line_num - 1])
                section_name = section_match.group(1).strip() if section_match else f"line_{line_num}"
                # Sanitize section name
                section_name = re.sub(r"[^\w\s-]", "", section_name)
                section_name = section_name.replace(" ", "-")
                section_name = section_name.replace("--", "-")  
                section_name = re.sub(r"-+", "-", section_name)
                url = f"https://github.com/{username}/{repo}/blob/master/{relative_path}#{section_name}"
            else:
                url = f"https://github.com/{username}/{repo}/blob/master/{relative_path}?plain=1#L{line_num}"
            _LOG.info(url)
    else:
        _LOG.info("Input not found in any Markdown files.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main(_parse())