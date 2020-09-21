#!/usr/bin/env python

"""
The script replaces all plantuml sections in the markdown files
with rendered images:

Usage:
    1. Include plantuml diagram picture to markdown:
    > render_md.py -i ABC.md -o XYZ.md --action render

    2. Include plantuml diagram picture in place:
    > render_md.py -i ABC.md --action render

    3. Open html to preview:
    > render_md.py -i ABC.md
    > render_md.py -i ABC.md --action open

    4. Combine 1-3:
    > render_md.py -i ABC.md -o XYZ.md--action open --action render

Details:
    - Read input file.
    - Extract the plantUML code started with ```plantuml and ended with ```.
    - Save the plantUML code in a temp file.
    - Call plantuml command on the temp file to create the uml image.
    - Store the image in a folder and return the link of the image.
    - Insert the plantUML code and the link of the image to the output file.
    - Process the rest of the file while looking for the plantuml code.
    - Open file for preview if needed.
"""

import os
import logging
import argparse
import tempfile
from typing import List

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

_ACTION_OPEN = 'open'
_ACTION_RENDER = 'render'
_VALID_ACTIONS = [_ACTION_OPEN, _ACTION_RENDER]
_DEFAULT_ACTIONS = [_ACTION_OPEN]


def _render_command(umlfile: str, umlpic: str) -> str:
    """Create PlantUML rendering command"""
    cmd = "plantuml %s > %s" % (umlfile, umlpic)
    return cmd


def _render_plantuml_code(umltext: str, out: str) -> None:
    """
    Render the PlantUML text into a file.

    :param umltext: UML format text
    :out: relative path to image file
    """
    # Format uml text to render.
    umlcontent = '@startuml\n%s\n@enduml' % umltext
    # Create the including directory, if needed.
    io_.create_enclosing_dir(out, incremental=True)
    # Save text to temporary file.
    with tempfile.TemporaryFile('w') as umlfile:
        # Format to be able to render.
        umlfile.write(umlcontent)
        # Convert the plantuml txt.
        si.system(_render_command(umlfile.name, out))


def _render_plantuml(in_txt: List[str], in_file: str) -> List[str]:
    """Add rendered image after plantuml code blocks"""
    # Store the output.
    out_txt: List[str] = []
    # Store the plantuml code found so far.
    plantuml_txt: List[str] = []
    # Count the index of the plantuml found in the file.
    plantuml_idx = 0
    # Store the state of the parser.
    state = "searching"
    dst_dir = "./plantuml-images"
    for i, line in enumerate(in_txt):
        _LOG.debug("%d: %s -> state=%s", i, line, state)
        out_txt.append(line)
        if line.strip() == "```plantuml":
            # Found the beginning of a plantuml text.
            dbg.dassert_eq(state, "searching")
            plantuml_txt = []
            plantuml_idx += 1
            state = "found_plantuml"
            _LOG.debug(" -> state=%s", state)
        elif line.strip() == "```" and state == "found_plantuml":
            # We want to assign the name of the image relative to the
            # originating file and index. In this way if we update the image,
            # the name of the image doesn't change.
            img_name = "%s.%s.png" % (in_file, plantuml_idx)
            img_name = os.path.join(dst_dir, in_file)
            _render_plantuml_code('\n'.join(plantuml_txt), img_name)
            out_txt.append("![](%s)" % img_name)
            state = "searching"
            _LOG.debug(" -> state=%s", state)
        elif line.strip != "```" and state == "found_plantuml":
            plantuml_txt.append(line)
    return out_txt


def _open_html(md_file: str) -> None:
    """Pandoc markdown to html and open it"""
    _LOG.info("\n%s", prnt.frame("Process markdown to html"))
    # Get pandoc.py command.
    curr_path = os.path.abspath(os.path.dirname(__file__))
    cmd = "%s/pandoc.py -t %s -i %s --skip_action %s" % (
        curr_path,
        "html",
        md_file,
        "copy_to_gdrive"
    )
    si.system(cmd)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_input_output_args(parser)
    prsr.add_action_arg(parser, _VALID_ACTIONS, _DEFAULT_ACTIONS)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Insert your code here.
    # Read file arguments.
    in_file, out_file = prsr.parse_input_output_args(args)
    # Not support stdin and stdout.
    dbg.dassert_ne(in_file, '-')
    dbg.dassert_ne(out_file, '-')
    # Read actions argument.
    actions = prsr.select_actions(args, _VALID_ACTIONS, _DEFAULT_ACTIONS)
    # Save to temporary file if only open.
    if actions == [_ACTION_OPEN]:
        out_file = tempfile.mktemp(suffix='.md')
    # Read input file lines.
    in_lines = io_.from_file(in_file).split('\n')
    # Get updated lines after rendering.
    out_lines = _render_plantuml(in_lines, in_file)
    # Save the output into a file.
    io_.to_file(out_file, out_lines)
    # Open if needed.
    if _ACTION_OPEN in actions:
        _open_html(out_file)


if __name__ == "__main__":
    _main(_parse())
