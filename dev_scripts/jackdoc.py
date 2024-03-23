#!/usr/bin/env python
"""
jackdoc: Locate input from Markdown files in the docs directory and generate corresponding file links.
Example usage:
jackdoc "search_term" [--skip-toc] [--line-only] [--subdir <subdirectory>]
"""
import argparse
import logging
import os
import re
import subprocess
from typing import List, Optional, Tuple
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio

_LOG = logging.getLogger(__name__)

# Define the relative path to the docs directory.
DOCS_DIR = "docs"

def _get_github_info() -> Tuple[str, str]:
    """
    Get GitHub repository information.
    """
    try:
        remote_url = (
            subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"]
            )
            .decode()
            .strip()
        )
        match = re.match(r"^https://github.com/(.*)/(.*)\.git$", remote_url)
        if match:
            return match.group(1), match.group(2)
    except (subprocess.CalledProcessError, IndexError) as exc:
        raise RuntimeError(
            "Unable to get remote URL from git configuration"
        ) from exc
    raise ValueError("Not a GitHub repository")

def _remove_toc(content: str) -> str:
    """
    Remove table of contents (TOC) from Markdown content.
    """
    toc_pattern = r"<!--\s*toc\s*-->(.*?)<!--\s*tocstop\s*-->"
    toc_match = re.search(toc_pattern, content, flags=re.DOTALL)
    if toc_match:
        toc_content = toc_match.group(1)
        toc_lines = toc_content.split("\n")
        excluded_lines = {
            content.count(toc_line, 0, toc_match.start(1)) + 1
            for toc_line in toc_lines
        }
        content_lines = content.split("\n")
        filtered_content = [
            line
            for i, line in enumerate(content_lines, start=1)
            if i not in excluded_lines
        ]
        return "\n".join(filtered_content)
    return content

def _search_in_markdown_files(
    git_root: str,
    search_term: str,
    skip_toc: bool = False,
    line_only: bool = False,
    subdir: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """
    Search for a term in Markdown files.
    """
    found_in_files = []
    docs_path = (
        os.path.join(git_root, DOCS_DIR, subdir) if subdir is not None
        else os.path.join(git_root, DOCS_DIR)
    )
    # Check if docs_path exists.
    hdbg.dassert_dir_exists(docs_path)
    def search_content(content: str) -> List[Tuple[str, str]]:
        if skip_toc:
            content = _remove_toc(content)
        if line_only:
            return [
                (md_file, str(line_num))
                for line_num, line in enumerate(content.split("\n"), start=1)
                if re.search(search_term, line)
            ]

        sections = re.findall(r"^#+\s+(.*)$", content, flags=re.MULTILINE)
        return [
            (md_file, section)
            for section in sections
            if re.search(search_term, section)
        ]

    for root, _, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)
                content = hio.from_file(md_file)
                found_in_files.extend(search_content(content))
    return found_in_files

def _main(parser: argparse.ArgumentParser) -> None:
    """
    Main function to parse command-line arguments and execute search.
    """
    args = parser.parse_args()
    git_root = hgit.get_client_root(super_module=True)
    username, repo = _get_github_info()
    found_items = _search_in_markdown_files(
        git_root, args.search_term, args.skip_toc, args.line_only, args.subdir
    )

    if found_items:
        _LOG.info("Input found in the following items:")
        for item in found_items:
            file_path, item_ref = item
            relative_path = os.path.relpath(file_path, git_root)
            if args.line_only:
                url = f"https://github.com/{username}/{repo}/blob/master/{relative_path}?plain=1#L{item_ref}"
            else:
                section_name = re.sub(r"[^\w\s-]", "", item_ref)
                section_name = section_name.replace(" ", "-").replace("--", "-")
                section_name = re.sub(r"-+", "-", section_name)
                url = f"https://github.com/{username}/{repo}/blob/master/{relative_path}#{section_name}"
            _LOG.info(url)
    else:
        _LOG.info("Input not found in any Markdown files.")

def _parse() -> argparse.ArgumentParser:
    """
    Parse command-line arguments.
    """
    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("search_term", help="Term to search in Markdown files")
    parser.add_argument(
        "--skip-toc",
        action="store_true",
        help="Skip search results from the table of contents (TOC)",
    )
    parser.add_argument(
        "--line-only",
        action="store_true",
        help="Search terms through the document and generate links with line numbers",
    )
    parser.add_argument("--subdir", help="Subdirectory to search within")
    return parser

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main(_parse())