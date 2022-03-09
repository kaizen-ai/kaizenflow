"""
Import as:

import helpers.hstring as hstring
"""
import logging
import tempfile
from typing import Optional, cast

import helpers.hio as hio
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def diff_strings(
    txt1: str,
    txt2: str,
    txt1_descr: Optional[str] = None,
    txt2_descr: Optional[str] = None,
    width: int = 130,
) -> str:
    # Write file.
    def _to_file(txt: str, txt_descr: Optional[str]) -> str:
        file_name = tempfile.NamedTemporaryFile().name
        if txt_descr is not None:
            txt = "# " + txt_descr + "\n" + txt
        hio.to_file(file_name, txt)
        return file_name

    file_name1 = _to_file(txt1, txt1_descr)
    file_name2 = _to_file(txt2, txt2_descr)
    # Get the difference between the files.
    cmd = f"sdiff --width={width} {file_name1} {file_name2}"
    _, txt = hsystem.system_to_string(
        cmd,
        # We don't care if they are different.
        abort_on_error=False,
    )
    # For some reason, mypy doesn't understand that system_to_string returns a
    # string.
    txt = cast(str, txt)
    return txt
