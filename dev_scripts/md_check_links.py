#!/usr/bin/env python

"""
Check the formatting of links and file/fig paths in Markdown files.
"""

import argparse
import logging
import os
import re
from typing import List, Tuple, Union

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


FIG_REGEX_1 = r'<img src="\.{0,2}\w*\/.+?\.(?:jpg|jpeg|png)"'
FIG_REGEX_2 = r"!\[\w*\]\(\.{0,2}\w*\/.+?\.(?:jpg|jpeg|png)\)"
FILE_PATH_REGEX = r"\.{0,2}\w*\/.+?\.[\w\.]+"
LINK_REGEX = r"\[(.+)\]\(((?!#).*)\)"


def _make_path_absolute(path: str) -> str:
    """
    Make the file path absolute.

    :param path: the original path
    :return: the absolute path
    """
    abs_path = path.lstrip("./")
    abs_path = "/" + abs_path
    return abs_path


def _make_path_module_agnostic(path: str) -> str:
    """
    Make the file path robust to where it is accessed from.

    E.g., when it is accessed from a submodule, it should
    include the `amp` directory explicitly.

    :param path: the original path
    :return: the module-agnostic path
    """
    # Get the absolute path of the `amp` dir.
    amp_path = hgit.get_amp_abs_path()
    # Compile the module-agnostic path.
    upd_path = os.path.join(amp_path, path.lstrip("/"))
    return upd_path


def _check_link_format(
    link_text: str, link: str, line: str, file_name: str, line_num: int
) -> Tuple[str, List[str]]:
    """
    Check whether the link is in an appropriate format.

    The desired format is '[/dir/file.py](/dir/file.py)':
      - The link text is the same as the link.
      - The link is an absolute path to the file (not a relative path and not a URL).

    If the original link text is a regular text and not a file path, it should not be updated.
    E.g., '[here](/dir/file.py)' remains as is.

    :param link_text: the original link text
    :param link: the original link
    :param line: the original line with the link
    :param file_name: the name of the Markdown file where the line is from
    :param line_num: the number of the line in the file
    :return:
        - the updated line with the link in the correct format
        - warnings about the issues with the link
    """
    warnings: List[str] = []
    old_link_txt = f"[{link_text}]({link})"
    if link == "" and (
        re.match(r"^{}$".format(FILE_PATH_REGEX), link_text)
        or link_text.startswith("http")
    ):
        # Fill in the empty link with the file path or URL from the link text.
        link = link_text
    if link == "":
        # The link is empty and there is no indication of how it should be filled;
        # update is impossible.
        return line, warnings
    if link.startswith("http"):
        if not any(x in link for x in ["cryptokaizen", "sorrentum"]):
            # The link is to an external resource; update is not needed.
            return line, warnings
        # Leave only the path to the file in the link.
        link = link.split("blob/master")[-1]
    # Make the path in the link absolute.
    link = _make_path_absolute(link)
    # Update the link text.
    if re.match(r"^{}$".format(FILE_PATH_REGEX), link_text):
        # Make the link text the same as link if the link text is a file path.
        link_text = link
    # Replace the link in the line with its updated version.
    new_link_txt = f"[{link_text}]({link})"
    updated_line = line.replace(old_link_txt, new_link_txt)
    if old_link_txt != new_link_txt:
        msg = (
            f"{file_name}:{line_num}: incorrectly formatted link '{old_link_txt}'"
        )
        warnings.append(msg)
    # Check that the file referenced by the link exists.
    link_in_cur_module = _make_path_module_agnostic(link)
    if not os.path.exists(link_in_cur_module):
        msg = f"{file_name}:{line_num}: '{link}' does not exist"
        warnings.append(msg)
    return updated_line, warnings


def _check_file_path_format(
    file_path: str, line: str, file_name: str, line_num: int
) -> Tuple[str, List[str]]:
    """
    Convert the file path into a link in a correct format.

    A file path like './dir/file.py' is converted into '[/dir/file.py](/dir/file.py)'.
      - The path to the file in the link should be absolute.

    :param file_path: the original file path
    :param line: the original line with the file path
    :param file_name: the name of the Markdown file where the line is from
    :param line_num: the number of the line in the file
    :return:
        - the updated line with the link to the file in the correct format
        - warnings about the issues with the link to the file
    """
    warnings: List[str] = []
    if not re.search(r'(?<!<img src=")(?<!\[)(?<!\()' + file_path, line):
        # Links and figure pointers are processed separately.
        return line, warnings
    if not re.search(r"(?<!http:)(?<!https:)" + file_path, line):
        # Ignore URLs.
        return line, warnings
    # Make the file path absolute.
    abs_file_path = _make_path_absolute(file_path)
    # Replace the bare file path in the line with the link to the file.
    link_with_file_path = f"[{abs_file_path}]({abs_file_path})"
    updated_line = line.replace(file_path, link_with_file_path)
    if file_path != link_with_file_path:
        msg = f"{file_name}:{line_num}: incorrectly formatted link '{file_path}'"
        warnings.append(msg)
    # Check that the file referenced by the link exists.
    abs_file_path_in_cur_module = _make_path_module_agnostic(abs_file_path)
    if not os.path.exists(abs_file_path_in_cur_module):
        msg = f"{file_name}:{line_num}: '{abs_file_path}' does not exist"
        warnings.append(msg)
    return updated_line, warnings


def _check_fig_pointer_format(
    fig_pointer: str, line: str, file_name: str, line_num: int
) -> Tuple[str, List[str]]:
    """
    Convert the pointer to a figure into a correct format.

    The desired format is '<img src="figs/dir/file.png">':
      - 'dir' is named the same as the Markdown file; this rule is not currently
        enforced but a warning is raised if it is not the case.

    :param fig_pointer: the original pointer to a figure
    :param line: the original line with the figure pointer
    :param file_name: the name of the Markdown file where the line is from
    :param line_num: the number of the line in the file
    :return:
        - the updated line with the figure pointer in the correct format
        - warnings about the issues with the figure pointer
    """
    warnings: List[str] = []
    # Extract the path to the figure from the pointer.
    fig_path = re.findall(FILE_PATH_REGEX, fig_pointer)[0]
    # Check the dir naming in the path.
    if not re.match(r"figs/{}/".format(file_name), fig_path):
        bname = os.path.basename(file_name)
        msg = f"{file_name}:{line_num}: '{fig_path}' does not follow the format 'figs/{bname}/XYZ'"
        warnings.append(msg)
    # Replace the figure pointer with the one in the correct format.
    updated_fig_pointer = f'<img src="{fig_path}"'
    if re.match(FIG_REGEX_2, fig_pointer):
        updated_fig_pointer += ">"
    updated_line = line.replace(fig_pointer, updated_fig_pointer)
    if fig_pointer != updated_fig_pointer:
        msg = (
            f"{file_name}:{line_num}: incorrectly formatted link '{fig_pointer}'"
        )
        warnings.append(msg)
    # Check that the file referenced by the pointer exists.
    dirname = _make_path_absolute(os.path.dirname(file_name))
    fig_path_abs = _make_path_absolute(fig_path)
    fig_path_abs_in_cur_module = _make_path_module_agnostic(fig_path_abs)
    dir_fig_path_abs_in_cur_module = _make_path_module_agnostic(
        os.path.join(dirname, fig_path_abs.lstrip("/"))
    )
    if not os.path.exists(fig_path_abs_in_cur_module) and not os.path.exists(
        dir_fig_path_abs_in_cur_module
    ):
        msg = f"{file_name}:{line_num}: '{fig_path}' does not exist"
        warnings.append(msg)
    return updated_line, warnings


def check_links(file_name: str, mode: str) -> Union[str, List[str]]:
    """
    Check the formatting of links and file/figure paths in a Markdown file.

    The following objects are checked:
      - Links in the Markdown format, e.g. '[link_text](link)'
        (incl. when the link text is an empty string).
      - Bare file paths, e.g.'/dir1/dir2/file.py'.
      - Pointers to figures, e.g. '<img src="dir1/dir2/file.png">'.

    :param file_name: the name of the Markdown file
    :param mode: what to do with the links in the file
        - "check": report if the links are incorrectly formatted
        - "fix": fix the format of incorrect links (this rewrites the original file)
    :return: the updated contents of the file or the warnings about incorrect links
    """
    lines = hio.from_file(file_name).split("\n")
    updated_lines: List[str] = []
    warnings: List[str] = []
    for i, line in enumerate(lines):
        updated_line = line
        # Check the formatting.
        # Links.
        link_matches = re.findall(LINK_REGEX, updated_line)
        for link_text, link in link_matches:
            updated_line, line_warnings = _check_link_format(
                link_text, link, updated_line, file_name, i
            )
            warnings.extend(line_warnings)
        # File paths.
        file_path_matches = re.findall(FILE_PATH_REGEX, updated_line)
        for file_path in file_path_matches:
            if not re.search(r"[a-zA-Z]", file_path):
                # Skip if there are no letters in the found path.
                continue
            if re.match(r"\.[a-zA-Z]", file_path):
                # Skip if the path is to a hidden file.
                continue
            updated_line, line_warnings = _check_file_path_format(
                file_path, updated_line, file_name, i
            )
            warnings.extend(line_warnings)
        # Figure pointers.
        fig_pointer_matches = re.findall(
            FIG_REGEX_1 + "|" + FIG_REGEX_2, updated_line
        )
        for fig_pointer in fig_pointer_matches:
            updated_line, line_warnings = _check_fig_pointer_format(
                fig_pointer, updated_line, file_name, i
            )
            warnings.extend(line_warnings)
        # Store the updated line.
        updated_lines.append(updated_line)
    if mode == "check":
        # Return the messages that report incorrectly formatted links.
        out: Union[str, List[str]] = [w for w in warnings if len(w)]
    elif mode == "fix":
        # Save the file with the correct link formatting.
        out = "\n".join(updated_lines)
        hio.to_file(file_name, out)
    else:
        raise ValueError(f"Unsupported mode='{mode}'")
    return out


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--mode",
        action="store",
        choices=["check", "fix"],
        help="'check' - report issues with incorrectly formatted links, 'fix' - fix them (rewrite)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input_files",
        action="append",
        help="Markdown files to check the links in",
    )
    group.add_argument(
        "--input_dir",
        action="store",
        help="Directory to check all Markdown files in",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level)
    # Get the Markdown files to check the links in.
    if args.input_files:
        files_to_check = []
        for input_file in args.input_files:
            hdbg.dassert_path_exists(args.input_file)
            hdbg.dassert_file_extension(args.input_file, ".md")
            files_to_check.append(input_file)
    elif args.input_dir:
        hdbg.dassert_dir_exists(args.input_dir)
        files_to_check = [
            os.path.join(dp, f)
            for dp, dn, fn in os.walk(args.input_dir)
            for f in fn
            if f.endswith(".md")
        ]
    # Run the link formatting check.
    for file_name in files_to_check:
        _ = check_links(file_name, args.mode)


if __name__ == "__main__":
    _main(_parse())
