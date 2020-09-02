#!/usr/bin/env python
r"""Replace aliased imports.

> p1_clean_import.py sample_file1.py sample_file2.py
"""
import argparse
import dataclasses
import logging
import re
from typing import Dict, List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


CUSTOM_SHORT_NAMES = {"pandas": "pd"}


@dataclasses.dataclass
class AliasedImport:
    import_path: str
    alias: str


def _get_aliased_imports(content: str) -> List[AliasedImport]:
    """Get a list of aliased imports.

    :param content: content of the file
    :return: a list of aliased imports
    """
    regex = r"import\s+(\S+)\s+as\s+(\S+)"
    return [
        AliasedImport(*match.groups()) for match in re.finditer(regex, content)
    ]


def _create_import_shortname(import_path: str) -> str:
    """Create a short name for the import based on the import path; either
    using a custom mapping, or following the transformation rule.

    :param import_path: complete path for the import
    :return: a short name for the import
    """
    if import_path in CUSTOM_SHORT_NAMES:
        return CUSTOM_SHORT_NAMES[import_path]

    shortname = ""

    parts = import_path.split(".")
    if len(parts) > 1:
        # Take the first letter of all parts except the last one.
        for p in parts[:-1]:
            shortname += p[0]

    # Add the first 5 letters in the last part without '\_'
    shortname += parts[-1].replace("_", "")

    return shortname


def _replace_alias(content: str, old_alias: str, new_alias: str) -> str:
    """Replace an alias inside content, returning the updated content. Should
    handle subscriptions.

    :param content: file contents as a string
    :param old_alias: alias to be replaced
    :param new_alias: alias to replace the old_alias with
    :return: the updated content
    """
    # ensures something.old_alias is not matched ref:
    # https://stackoverflow.com/a/39685053
    negative_look_behind = r"(?<!\.)"

    regex = r"\b" + negative_look_behind + old_alias + r"\b"

    try:
        content = re.sub(regex, new_alias, content)
    except re.error:
        _LOG.exception("Failed to replace '%s' with '%s'", old_alias, new_alias)

    return content


def replace_aliases_in_file(file_name: str) -> None:
    """Process a file, replacing its content in place with the updated aliases.

    :param file_name: name of the file to process
    """
    _LOG.info("Cleaning imports in '%s'.", file_name)

    content = io_.from_file(file_name=file_name)
    aliased_imports = _get_aliased_imports(content=content)

    for aliased_import in aliased_imports:
        # Use a transformation rule to transform the alias name.
        new_alias = _create_import_shortname(
            import_path=aliased_import.import_path
        )
        # Replace all instances of the alias with the transformed aliases for that file.
        content = _replace_alias(
            content=content, old_alias=aliased_import.alias, new_alias=new_alias
        )

    io_.to_file(file_name=file_name, lines=content)


def _check_alias_taken(content: str, alias: str) -> bool:
    """Check if an alias is already used in the content, either as an alias or
    a variable.

    :param content: file content
    :param alias: alias to check
    :return: True if alias is used
    """
    import_regex = r"as\s+" + alias + r"\b"
    assignment_regex = r"\b" + alias + r"\s*="
    all_regex = [import_regex, assignment_regex]
    return any([re.search(regex, content) for regex in all_regex])


def _check_conflict(content: str, old_alias: str, new_alias: str) -> bool:
    """Check if a conflict exists due to the following conditions.

    - the old alias doesn't match the new alias
    - and the new alias is already used in the file

    :param content: file content
    :param alias: alias to check
    :return: True if a conflict exists
    """
    return all(
        [
            old_alias != new_alias,
            _check_alias_taken(content=content, alias=new_alias),
        ]
    )


def check_mapping(file_name: str) -> Dict[str, str]:
    """Generate a mapping between import paths and shortnames for a file, and
    checks its content.

    :param import_paths: a Iterable of import paths
    :return: a mapping between import paths and short names
    :raises AssertionError: when a conflict exists in a file
    """
    mapping: Dict[str, str] = dict()

    content = io_.from_file(file_name=file_name)
    aliased_imports = _get_aliased_imports(content=content)

    for aliased_import in aliased_imports:
        import_path = aliased_import.import_path
        short_name = _create_import_shortname(import_path)

        mapping[import_path] = short_name

        conflict = _check_conflict(
            content=content, old_alias=aliased_import.alias, new_alias=short_name
        )

        dbg.dassert(
            not conflict,
            msg=f"alias '{short_name}' is already used in '{file_name}'",
        )

    return mapping


class _P1CleanImport(lntr.Action):
    def __init__(self, enforce: bool):
        self._enforce = enforce
        super().__init__("")

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        mapping = check_mapping(file_name)

        for import_path, short_name in mapping.items():
            _LOG.debug("Mapping '%s' -> '%s'", import_path, short_name)

        if self._enforce:
            replace_aliases_in_file(file_name)

        return []


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dont-enforce",
        action="store_true",
        default=False,
        help="Replace aliases.",
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process"
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _P1CleanImport(not args.dont_enforce)
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
