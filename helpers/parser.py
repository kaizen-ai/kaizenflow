"""
Import as:

import helpers.parser as prsr
"""

import argparse
from typing import List, Optional

import helpers.dbg as dbg


def add_bool_arg(
    parser: argparse.ArgumentParser,
    name,
    default: bool = False,
    help_: Optional[str] = None,
) -> argparse.ArgumentParser:
    """
    Add options to a parser like --xyz and --no-xyz (e.g., for --incremental).
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


def _actions_to_string(actions: List[str], valid_actions: List[str]) -> str:
    space = max([len(a) for a in valid_actions]) + 2
    format_ = "%" + str(space) + "s: %s"
    actions_as_str = [
        format_ % (a, "Yes" if a in actions else "-") for a in valid_actions
    ]
    return "\n".join(actions_as_str)


def select_actions(
    args: argparse.Namespace, valid_actions: List[str]
) -> List[str]:
    # Select actions.
    if not args.action or args.all:
        actions = list(valid_actions)
    else:
        actions = args.action
    actions = actions[:]
    dbg.dassert_isinstance(actions, list)
    # Validate actions.
    for action in set(actions):
        if action not in valid_actions:
            raise ValueError("Invalid action '%s'" % action)
    # Reorder actions according to _ALL_ACTIONS.
    actions = [action for action in valid_actions if action in actions]
    # # Find the tools that are available.
    # actions = _remove_not_possible_actions(actions)
    # #
    # actions_as_str = _actions_to_string(actions)
    # _LOG.info("# Action selected:\n%s", pri.space(actions_as_str))
    return actions


def add_action_arg(
    parser: argparse.ArgumentParser, valid_actions: List[str]
) -> argparse.ArgumentParser:
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--action",
        dest="action",
        action="append",
        choices=valid_actions,
        help="Actions to execute",
    )
    group.add_argument(
        "--skip-action",
        dest="skip_action",
        action="append",
        choices=valid_actions,
        help="Actions to skip",
    )
    parser.add_argument("--all", action="store_true", help="Run all the actions")
    return parser
