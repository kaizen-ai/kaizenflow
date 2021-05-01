import logging
import re
from typing import Any, Dict, List, Tuple

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)

CFILE_ROW = Tuple[str, int, str]


def cfile_row_to_str(cfile_row: CFILE_ROW) -> str:
    # helpers/git.py:295:def get_repo_long_name_from_client(super_module
    dbg.dassert_isinstance(cfile_row, tuple)
    return ":".join(list(map(str, cfile_row)))


def cfile_to_str(cfile: List[CFILE_ROW]) -> str:
    dbg.dassert_isinstance(cfile, list)
    return "\n".join(map(cfile_row_to_str, cfile))


def parse_traceback(txt: str) -> List[CFILE_ROW]:
    lines = txt.split("\n")
    state = "look_for"
    cfile: List[CFILE_ROW] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        _LOG.debug("state=%-10s i=%d: line=%s", state, i, line)
        if state == "look_for":
            if line.startswith("Traceback (most recent call last):"):
                # Update the state.
                state = "parse"
                i += 1
                continue
        elif state == "parse":
            # The file looks like:
            #   File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh
            #     act = ltasks._get_gh_issue_title(issue_id, repo)
            regex = r"^\s+File \"(\S+)\", line (\d+), in (\S+)$"
            m = re.match(regex, line)
            dbg.dassert(m, "Can't parse '%s'", line)
            file_name = m.group(1)
            line_num = int(m.group(2))
            func_name = m.group(3)
            _LOG.debug("  -> %s %d %s", file_name, line_num, func_name)
            # Parse the next line until the next `File...`.
            _LOG.debug("Search end of snippet")
            j = i + 1
            dbg.dassert_lte(j, len(lines))
            while j < len(lines):
                _LOG.debug("  j=%d: line=%s", j, lines[j])
                if lines[j].startswith("  File \"") or not lines[j].startswith("  "):
                    _LOG.debug("  Found end of snippet")
                    break
                j += 1
            # Concatenate the lines into a single line.
            code = lines[i + 1:j]
            _LOG.debug("  -> code: [%d, %d]\n%s", i, j, "\n".join(code))
            code = map(lambda x: x.rstrip().lstrip(), code)
            code_as_single_line = "/".join(code)
            _LOG.debug("  -> code_as_single_line=\n%s", code_as_single_line)
                       # Assemble the result.
            cfile_row = (file_name, line_num, func_name + ":" + code_as_single_line)
            _LOG.debug("  => cfile_row='%s'", cfile_row_to_str(cfile_row))
            cfile.append(cfile_row)
            # Update the state.
            if not lines[j].startswith("  "):
                _LOG.debug("  Found end of traceback")
                break
            else:
                state = "parse"
                i = j
                continue
        #
        i += 1
    return cfile
