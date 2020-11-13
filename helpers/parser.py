"""Import as:

import helpers.parser as prsr
"""

import argparse
import logging
import os
import sys
from typing import List, Optional, Tuple

import helpers.dbg as dbg
import helpers.printing as prnt

_LOG = logging.getLogger(__name__)


def add_bool_arg(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool = False,
    help_: Optional[str] = None,
) -> argparse.ArgumentParser:
    """Add options to a parser like --xyz and --no_xyz.

    E.g., for `--incremental`.
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--" + name, dest=name, action="store_true", help=help_)
    group.add_argument("--no_" + name, dest=name, action="store_false")
    parser.set_defaults(**{name: default})
    return parser


def add_verbosity_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


# #############################################################################


def add_action_arg(
    parser: argparse.ArgumentParser,
    valid_actions: List[str],
    default_actions: List[str],
) -> argparse.ArgumentParser:
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--action",
        action="append",
        choices=valid_actions,
        help="Actions to execute",
    )
    group.add_argument(
        "--skip_action",
        action="append",
        choices=valid_actions,
        help="Actions to skip",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all the actions (%s)" % (" ".join(default_actions)),
    )
    return parser


def actions_to_string(
    actions: List[str], valid_actions: List[str], add_frame: bool
) -> str:
    space = max([len(a) for a in valid_actions]) + 2
    format_ = "%" + str(space) + "s: %s"
    actions = [
        format_ % (a, "Yes" if a in actions else "-") for a in valid_actions
    ]
    actions_as_str = "\n".join(actions)
    if add_frame:
        ret = prnt.frame("# Action selected:") + "\n"
        ret += prnt.indent(actions_as_str)
    else:
        ret = actions_as_str
    return ret  # type: ignore


def select_actions(
    args: argparse.Namespace, valid_actions: List[str], default_actions: List[str]
) -> List[str]:
    dbg.dassert(
        not (args.action and args.all),
        "You can't specify together --action and --all",
    )
    dbg.dassert(
        not (args.action and args.skip_action),
        "You can't specify together --action and --skip_action",
    )
    # Select actions.
    if not args.action or args.all:
        if default_actions is None:
            default_actions = valid_actions[:]
        dbg.dassert_is_subset(default_actions, valid_actions)
        # Convert it into list since through some code paths it can be a tuple.
        actions = list(default_actions)
    else:
        actions = args.action[:]
    dbg.dassert_isinstance(actions, list)
    dbg.dassert_no_duplicates(actions)
    # Validate actions.
    for action in set(actions):
        if action not in valid_actions:
            raise ValueError("Invalid action '%s'" % action)
    # Remove actions, if needed.
    if args.skip_action:
        dbg.dassert_isinstance(args.skip_action, list)
        for skip_action in args.skip_action:
            dbg.dassert_in(skip_action, actions)
            actions = [a for a in actions if a != skip_action]
    # Reorder actions according to 'valid_actions'.
    actions = [action for action in valid_actions if action in actions]
    return actions


def mark_action(action: str, actions: List[str]) -> Tuple[bool, List[str]]:
    to_execute = action in actions
    _LOG.debug("\n%s", prnt.frame("action=%s" % action))
    if to_execute:
        actions = [a for a in actions if a != action]
    else:
        _LOG.warning("Skip action='%s'", action)
    return to_execute, actions


# #############################################################################


def add_input_output_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """Add options to parse input and output file name."""
    parser.add_argument(
        "-i",
        "--in_file_name",
        required=True,
        type=str,
        help="Input file or `-` for stdin",
    )
    parser.add_argument(
        "-o",
        "--out_file_name",
        required=False,
        type=str,
        default=None,
        help="Output file or `-` for stdout",
    )
    return parser


def parse_input_output_args(
    args: argparse.Namespace, clear_screen: bool = True
) -> Tuple[str, str]:
    in_file_name = args.in_file_name
    out_file_name = args.out_file_name
    if out_file_name is None:
        out_file_name = in_file_name
    # Print summary.
    if in_file_name != "-":
        if clear_screen:
            os.system("clear")
        print("in_file_name='%s'" % in_file_name)
        print("out_file_name='%s'" % out_file_name)
    return in_file_name, out_file_name


def read_file(file_name: str) -> List[str]:
    """Read file or stdin (represented by `-`), returning an array of lines."""
    if file_name == "-":
        f = sys.stdin
    else:
        f = open(file_name, "r")
    # Read.
    txt = []
    for line in f:
        line = line.rstrip("\n")
        txt.append(line)
    f.close()
    return txt


def write_file(txt: List[str], file_name: str) -> None:
    """Write txt in a file or stdin (represented by `-`)."""
    if file_name == "-":
        print("\n".join(txt))
    else:
        with open(file_name, "w") as f:
            f.write("\n".join(txt))
