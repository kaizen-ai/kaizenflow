"""
Import as:

import helpers.open as opn
"""

import logging
import os

import helpers.dbg as dbg
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _cmd_open_html(file_name: str, os_name: str) -> str:
    """Get OS-based command to open html file."""
    os_cmds = {
        "Darwin": "open",
        "Windows": "start",
        "Linux": "xdg-open",
    }
    dbg.dassert_in(os_name, os_cmds)
    full_cmd = "%s %s" % (os_cmds[os_name], file_name)
    return full_cmd


def _cmd_open_pdf(file_name: str, os_name: str) -> str:
    """Get OS-based command to open pdf file."""
    os_full_cmds = {
        "Darwin": "/usr/bin/osascript << EOF\n"
        'set theFile to POSIX file "%s" as alias\n'
        'tell application "Skim"\n'
        "activate\n"
        "set theDocs to get documents whose path is "
        "(get POSIX path of theFile)\n"
        "if (count of theDocs) > 0 then revert theDocs\n"
        "open theFile\n"
        "end tell\n"
        "EOF" % file_name
    }
    dbg.dassert_in(os_name, os_full_cmds)
    return os_full_cmds[os_name]


def open_file(file_name: str) -> None:
    """Open file if extension is supported"""
    # Define file format.
    suffix = os.path.split(file_name)[-1].split(".")[-1]
    # Check file.
    _LOG.info(
        "\n%s", prnt.frame("Open %s" % suffix.upper(), char1="<", char2=">")
    )
    dbg.dassert_exists(file_name)
    _LOG.debug("Opening file='%s'", file_name)
    # Define OS.
    os_name = si.get_os_name()
    # Define open command for OS.
    cmd: str
    if suffix == "pdf":
        cmd = _cmd_open_pdf(file_name, os_name)
    elif suffix == "html":
        cmd = _cmd_open_html(file_name, os_name)
    else:
        dbg.dassert(False, "Open .%s files is not supported yet." % suffix)
    # Run command.
    si.system(cmd)
