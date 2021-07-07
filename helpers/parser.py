"""
Import as:

import helpers.parser as hparse
"""

import argparse
import logging
import os
import sys
from typing import Any, List, Optional, Tuple

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)

# TODO(gp): arg -> args


# #############################################################################


def add_bool_arg(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool = False,
    help_: Optional[str] = None,
) -> argparse.ArgumentParser:
    """
    Add options to a parser like --xyz and --no_xyz.

    E.g., for `--incremental`.
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--" + name, dest=name, action="store_true", help=help_)
    group.add_argument("--no_" + name, dest=name, action="store_false")
    parser.set_defaults(**{name: default})
    return parser


# #############################################################################


def add_verbosity_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


# TODO(gp): Use this everywhere.
def parse_verbosity_args(
    args: argparse.Namespace, *args_: Any, **kwargs: Any
) -> None:
    dbg.init_logger(verbosity=args.log_level, *args_, **kwargs)


# #############################################################################
# Command line options for handling the destination dir.
# #############################################################################


def add_dst_dir_arg(
    parser: argparse.ArgumentParser,
    dst_dir_required: bool,
    dst_dir_default: Optional[str] = None,
) -> argparse.ArgumentParser:
    """
    Add command line options related to destination dir.

    E.g., `--dst_dir`, `--clean_dst_dir`
    """
    # TODO(gp): Add unit test to check this.
    # A required dst_dir implies no default dst_dir.
    dbg.dassert_imply(
        dst_dir_required,
        not dst_dir_default,
        "Since dst_dir_required='%s', you need to specify a default "
        "destination dir, instead of dst_dir_default='%s'",
        dst_dir_required,
        dst_dir_default,
    )
    # If dst_dir is not required, then a default dst_dir must be specified.
    dbg.dassert_imply(
        not dst_dir_required,
        dst_dir_default,
        "Since dst_dir_required='%s', you can't specify a default "
        "destination dir, dst_dir_default='%s'",
        dst_dir_required,
        dst_dir_default,
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        default=dst_dir_default,
        required=dst_dir_required,
        help="Directory storing the results",
    )
    parser.add_argument(
        "--clean_dst_dir",
        action="store_true",
        help="Delete the destination dir before running",
    )
    parser.add_argument(
        "--no_confirm",
        action="store_true",
        help="Do not confirm before deleting dst dir",
    )
    return parser


def parse_dst_dir_arg(args: argparse.Namespace) -> Tuple[str, bool]:
    """
    Process the command line options related to destination dir.

    :return: a tuple (dst_dir, clean_dst_dir)
        - dst_dir: the destination dir
        - clean_dst_dir: whether to clean the destination dir or not
    """
    dst_dir = args.dst_dir
    _LOG.debug("dst_dir=%s", dst_dir)
    clean_dst_dir = False
    if args.clean_dst_dir:
        if os.path.exists(dst_dir):
            _LOG.warning("Found that dir '%s' already exists", dst_dir)
            if not args.no_confirm:
                clean_dst_dir = hsyste.query_yes_no(
                    "Do you want to delete the dir '%s'" % dst_dir,
                    abort_on_no=True,
                )
    hio.create_dir(dst_dir, incremental=True)
    _LOG.debug("clean_dst_dir=%s", clean_dst_dir)
    return dst_dir, clean_dst_dir


# #############################################################################
# Command line options related to selection actions.
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
        ret = hprint.frame("# Action selected:") + "\n"
        ret += hprint.indent(actions_as_str)
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
    _LOG.debug("\n%s", hprint.frame("action=%s" % action))
    if to_execute:
        actions = [a for a in actions if a != action]
    else:
        _LOG.warning("Skip action='%s'", action)
    return to_execute, actions


# #############################################################################
# Command line options for input/output processing.
# #############################################################################


def add_input_output_args(
    parser: argparse.ArgumentParser,
    in_default: Optional[str] = None,
    out_default: Optional[str] = None,
) -> argparse.ArgumentParser:
    """
    Add options to parse input and output file name.

    :param in_default: default file to be used for input
        - If `None`, it must be specified by the user
    :param in_default: same as `in_default` but for output
    """
    parser.add_argument(
        "-i",
        "--in_file_name",
        required=(in_default is None),
        type=str,
        default=in_default,
        help="Input file or `-` for stdin",
    )
    parser.add_argument(
        "-o",
        "--out_file_name",
        required=(out_default is None),
        type=str,
        default=out_default,
        help="Output file or `-` for stdout",
    )
    return parser


def parse_input_output_args(
    args: argparse.Namespace, clear_screen: bool = False
) -> Tuple[str, str]:
    """
    :return input and output file name
    """
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
    """
    Read file or stdin (represented by `-`), returning an array of lines.
    """
    if file_name == "-":
        _LOG.info("Reading from stdin")
        f = sys.stdin
    else:
        _LOG.info("Reading from '%s'", file_name)
        # pylint: disable=consider-using-with
        f = open(file_name, "r")
    # Read.
    txt = []
    for line in f:
        line = line.rstrip("\n")
        txt.append(line)
    f.close()
    return txt


def write_file(txt: List[str], file_name: str) -> None:
    """
    Write txt in a file or stdout (represented by `-`).
    """
    if file_name == "-":
        print("\n".join(txt))
    else:
        with open(file_name, "w") as f:
            f.write("\n".join(txt))
        _LOG.info("Written file '%s'", file_name)


# #############################################################################
# Command line options for parallel processing.
# #############################################################################


def add_parallel_processing_arg(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--num_threads",
        action="store",
        help="""
Number of threads to use:
- '-1' to use all CPUs;
- '1' to use one-thread at the time but using the parallel execution;
- 'serial' to serialize the execution without using parallel execution""",
        required=True,
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print workload and exit without running",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Skip workload already performed",
    )
    parser.add_argument(
        "--skip_on_error",
        action="store_true",
        help="Continue execution after encountering an error",
    )
    parser.add_argument(
        "--num_attempts",
        default=1,
        type=int,
        help="Repeat running an experiment up to `num_attempts` times",
        required=False,
    )
    return parser
