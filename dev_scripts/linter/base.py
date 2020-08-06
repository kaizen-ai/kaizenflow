#!/usr/bin/env python
"""Reformat and lint python and ipynb files.

This script uses the version of the files present on the disk and not what is
staged for commit by git, thus you need to stage again after running it.

E.g.,
# Lint all modified files in git client.
> linter.py

# Lint current files.
> linter.py -c --collect_only
> linter.py -c --all

# Lint previous commit files.
> linter.py -p --collect_only

# Lint a certain number of previous commits.
> linter.py -p 3 --collect_only
> linter.py --files event_study/*.py linter_v2.py -v DEBUG

# Lint the changes in the branch.
> linter.py -b
> linter.py -f $(git diff --name-only master...)

# Lint all python files, but not the notebooks.
> linter.py -d . --only_py --collect

# To jump to all the warnings to fix.
> vim -c "cfile linter.log"

# Check all jupytext files.
> linter.py -d . --action sync_jupytext
"""

import argparse
import itertools
import logging
import os
import re
import sys
from typing import Any, List, Tuple, Type

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.list as hlist
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si
import linter.utils as utils

_LOG = logging.getLogger(__name__)

# Use the current dir and not the dir of the executable.
_TMP_DIR = os.path.abspath(os.getcwd() + "/tmp.linter")


# #############################################################################
# Utils.
# #############################################################################


NO_PRINT = False


def _print(*args: Any, **kwargs: Any) -> None:
    if not NO_PRINT:
        print(*args, **kwargs)


def _annotate_output(output: List, executable: str) -> List:
    """Annotate a list containing the output of a cmd line with the name of the
    executable used.

    :return: list of strings
    """
    prnt.dassert_list_of_strings(output)
    output = [t + " [%s]" % executable for t in output]
    prnt.dassert_list_of_strings(output)
    return output


# TODO(saggese): should this be moved to system interactions?
def tee(cmd: str, executable: str, abort_on_error: bool) -> Tuple[int, List[str]]:
    """Execute command "cmd", capturing its output and removing empty lines.

    :return: list of strings
    """
    _LOG.debug("cmd=%s executable=%s", cmd, executable)
    rc, output = si.system_to_string(cmd, abort_on_error=abort_on_error)
    dbg.dassert_isinstance(output, str)
    output1 = output.split("\n")
    _LOG.debug("output1= (%d)\n'%s'", len(output1), "\n".join(output1))
    #
    output2 = prnt.remove_empty_lines_from_string_list(output1)
    _LOG.debug("output2= (%d)\n'%s'", len(output2), "\n".join(output2))
    prnt.dassert_list_of_strings(output2)
    return rc, output2


# #############################################################################
# Handle files.
# #############################################################################


def _filter_target_files(file_names: List[str]) -> List[str]:
    """Keep only the files that:

    - have extension .py, .ipynb, .txt or .md.
    - are not Jupyter checkpoints
    - are not in tmp dirs
    """
    file_names_out: List[str] = []
    for file_name in file_names:
        _, file_ext = os.path.splitext(file_name)
        # We skip .ipynb since jupytext is part of the main flow.
        is_valid = file_ext in (".py", ".txt", ".md")
        is_valid &= ".ipynb_checkpoints/" not in file_name
        is_valid &= "dev_scripts/install/conda_envs" not in file_name
        # Skip requirements.txt.
        is_valid &= os.path.basename(file_name) != "requirements.txt"
        # Skip tmp names since we need to run on unit tests.
        if False:
            # Skip files in directory starting with "tmp.".
            is_valid &= "/tmp." not in file_name
            # Skip files starting with "tmp.".
            is_valid &= not file_name.startswith("tmp.")
        if is_valid:
            file_names_out.append(file_name)
    return file_names_out


def _get_files(args: argparse.Namespace) -> List[str]:
    """Return the list of files to process given the command line arguments."""
    file_names: List[str] = []
    if args.files:
        _LOG.debug("Specified files")
        # User has specified files.
        file_names = args.files
    else:
        if args.current_git_files:
            # Get all the git modified files.
            file_names = git.get_modified_files()
        elif args.previous_git_committed_files is not None:
            _LOG.debug("Looking for files committed in previous Git commit")
            # Get all the git in user previous commit.
            n_commits = args.previous_git_committed_files
            _LOG.info("Using %s previous commits", n_commits)
            file_names = git.get_previous_committed_files(n_commits)
        elif args.modified_files_in_branch:
            dir_name = "."
            dst_branch = "master"
            file_names = git.get_modified_files_in_branch(dir_name, dst_branch)
        elif args.target_branch:
            dir_name = "."
            dst_branch = args.target_branch
            file_names = git.get_modified_files_in_branch(dir_name, dst_branch)
        elif args.dir_name:
            if args.dir_name == "$GIT_ROOT":
                dir_name = git.get_client_root(super_module=True)
            else:
                dir_name = args.dir_name
            dir_name = os.path.abspath(dir_name)
            _LOG.info("Looking for all files in '%s'", dir_name)
            dbg.dassert_exists(dir_name)
            cmd = "find %s -name '*' -type f" % dir_name
            _, output = si.system_to_string(cmd)
            file_names = output.split("\n")
    # Remove text files used in unit tests.
    file_names = [f for f in file_names if not utils.is_test_input_output_file(f)]
    # Make all paths absolute.
    ## file_names = [os.path.abspath(f) for f in file_names]
    # Check files exist.
    file_names_out = []
    for f in file_names:
        if not os.path.exists(f):
            _LOG.warning("File '%s' doesn't exist: skipping", f)
        else:
            file_names_out.append(f)
    return file_names_out


def _list_to_str(list_: List[str]) -> str:
    return "%d (%s)" % (len(list_), " ".join(list_))


def _get_files_to_lint(
    args: argparse.Namespace, file_names: List[str]
) -> List[str]:
    """Get all the files that need to be linted.

    Typically files to lint are python and notebooks.
    """
    _LOG.debug("file_names=%s", _list_to_str(file_names))
    # Keep only actual .py and .ipynb files.
    file_names = _filter_target_files(file_names)
    _LOG.debug("file_names=%s", _list_to_str(file_names))
    # Remove files.
    if args.skip_py:
        file_names = [f for f in file_names if not utils.is_py_file(f)]
    if args.skip_ipynb:
        file_names = [f for f in file_names if not utils.is_ipynb_file(f)]
    if args.skip_files:
        dbg.dassert_isinstance(args.skip_files, list)
        # TODO(gp): Factor out this code and reuse it in this function.
        _LOG.warning(
            "Skipping %s files, as per user request",
            _list_to_str(args.skip_files),
        )
        skip_files = args.skip_files
        skip_files = [os.path.abspath(f) for f in skip_files]
        skip_files = set(skip_files)
        file_names_out = [f for f in file_names if f not in skip_files]
        removed_file_names = list(set(file_names) - set(file_names_out))
        _LOG.warning("Removing %s files", _list_to_str(removed_file_names))
        file_names = file_names_out
    # Keep files.
    if args.only_py:
        file_names = [
            f
            for f in file_names
            if utils.is_py_file(f) and not utils.is_paired_jupytext_file(f)
        ]
    if args.only_ipynb:
        file_names = [f for f in file_names if utils.is_ipynb_file(f)]
    if args.only_paired_jupytext:
        file_names = [f for f in file_names if utils.is_paired_jupytext_file(f)]
    #
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    if len(file_names) < 1:
        _LOG.warning("No files that can be linted are specified")
    return file_names


# #############################################################################


# There are some lints that:
#   A) we disagree with (e.g., too many functions in a class)
#       - they are ignored all the times.
#   B) are too hard to respect (e.g., each function has a docstring)
#       - they are ignored unless we want to see them.
#
# Pedantic=2 -> all lints, including a) and b)
# Pedantic=1 -> discard lints from a), include b)
# Pedantic=0 -> discard lints from a) and b)

# - The default is to run with (-> pedantic=0)
# - Sometimes we want to take a look at the lints that we would like to enforce.
#   (-> pedantic=1)
# - In rare occasions we want to see all the lints (-> pedantic=2)


# TODO(gp): joblib asserts when using abstract classes:
#   AttributeError: '_BasicHygiene' object has no attribute '_executable'
## class Action(abc.ABC):
class Action:
    """Implemented as a Strategy pattern."""

    def __init__(self, executable: str = "") -> None:
        self._executable = executable

    ## @abc.abstractmethod
    def check_if_possible(self) -> bool:
        """Check if the action can be executed."""
        raise NotImplementedError

    def execute(self, file_name: str, pedantic: int) -> List[str]:
        """Execute the action.

        :param file_name: name of the file to process
        :param pendantic: True if it needs to be run in angry mode
        :return: list of strings representing the output
        """
        dbg.dassert(file_name)
        dbg.dassert_exists(file_name)
        output = self._execute(file_name, pedantic)
        prnt.dassert_list_of_strings(output)
        return output

    ## @abc.abstractmethod
    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        raise NotImplementedError


def run_action(action: Action, file_names: List[str]) -> None:
    full_output: List[str] = []
    for file_name in file_names:
        dbg.dassert_exists(file_name)
        output = action._execute(  # pylint: disable=protected-access; temporary
            file_name, pedantic=0
        )
        full_output.extend(output)
    # Print the output.
    prnt.dassert_list_of_strings(full_output)
    print("\n".join(full_output))
    # Exit.
    rc = len(full_output)
    sys.exit(rc)


# #############################################################################


def _check_file_property(
    actions: List[str], all_file_names: List[str], pedantic: int
) -> Tuple[List[str], List[str]]:
    output: List[str] = []
    action = "check_file_property"
    if action in actions:
        for file_name in all_file_names:
            class_ = _get_action_class(action)
            output_tmp = class_.execute(file_name, pedantic)
            prnt.dassert_list_of_strings(output_tmp)
            output.extend(output_tmp)
    actions = [a for a in actions if a != action]
    _LOG.debug("actions=%s", actions)
    return output, actions


# #############################################################################
# Actions.
# #############################################################################

# We use the command line instead of API because:
# - some tools don't have a public API.
# - this make easier to reproduce / test commands using the command lines.
# - once tested, we can then incorporate in the code.
# - it allows us to have clear control over options.


# Actions and if they read / write files.
# The order of this list implies the order in which they are executed.

# TODO(GP,Sergey): I think this info should be encapsulated in classes.
# There are mapping that we have to maintain. DRY.
_VALID_ACTIONS_META: List[Tuple[str, str, str, Type[Action]]] = []


# Joblib and caching with lru_cache don't get along, so we cache explicitly.
_VALID_ACTIONS = None


def _get_valid_actions() -> List[str]:
    global _VALID_ACTIONS
    if _VALID_ACTIONS is None:
        _VALID_ACTIONS = list(zip(*_VALID_ACTIONS_META))[0]
    return _VALID_ACTIONS  # type: ignore


def _get_default_actions() -> List[str]:
    return _get_valid_actions()


def _get_action_class(action: str) -> Action:
    """Return the function corresponding to the passed string."""
    res = None
    for action_meta in _VALID_ACTIONS_META:
        name, rw, comment, class_ = action_meta
        _ = rw, comment
        if name == action:
            dbg.dassert_is(res, None)
            res = class_
    dbg.dassert_is_not(res, None)
    # Mypy gets confused since we are returning a class.
    obj = res()  # type: ignore
    return obj


def _remove_not_possible_actions(actions: List[str]) -> List[str]:
    """Check whether each action in "actions" can be executed and return a list
    of the actions that can be executed.

    :return: list of strings representing actions
    """
    actions_tmp: List[str] = []
    for action in actions:
        class_ = _get_action_class(action)
        is_possible = class_.check_if_possible()
        if not is_possible:
            _LOG.warning("Can't execute action '%s': skipping", action)
        else:
            actions_tmp.append(action)
    return actions_tmp


def _select_actions(args: argparse.Namespace) -> List[str]:
    valid_actions = _get_valid_actions()
    default_actions = _get_default_actions()
    actions = prsr.select_actions(args, valid_actions, default_actions)
    # Find the tools that are available.
    actions = _remove_not_possible_actions(actions)
    #
    add_frame = True
    actions_as_str = prsr.actions_to_string(
        actions, _get_valid_actions(), add_frame
    )
    _LOG.info("\n%s", actions_as_str)
    return actions


def _test_actions() -> None:
    _LOG.info("Testing actions")
    # Check all the actions.
    num_not_poss = 0
    possible_actions: List[str] = []
    for action in _get_valid_actions():
        class_ = _get_action_class(action)
        is_possible = class_.check_if_possible()
        _LOG.debug("%s -> %s", action, is_possible)
        if is_possible:
            possible_actions.append(action)
        else:
            num_not_poss += 1
    # Report results.
    add_frame = True
    actions_as_str = prsr.actions_to_string(
        possible_actions, _get_valid_actions(), add_frame
    )
    _LOG.info("\n%s", actions_as_str)
    if num_not_poss > 0:
        _LOG.warning("There are %s actions that are not possible", num_not_poss)
    else:
        _LOG.info("All actions are possible")


# #############################################################################


def _lint(
    file_name: str, actions: List[str], pedantic: int, debug: bool
) -> List[str]:
    """Execute all the actions on a filename.

    Note that this is the unit of parallelization, i.e., we run all the
    actions on a single file to ensure that the actions are executed in
    the proper order.
    """
    output: List[str] = []
    _LOG.info("\n%s", prnt.frame(file_name, char1="="))
    for action in actions:
        _LOG.debug("\n%s", prnt.frame(action, char1="-"))
        _print("## %-20s (%s)" % (action, file_name))
        if debug:
            # Make a copy after each action.
            dst_file_name = file_name + "." + action
            cmd = "cp -a %s %s" % (file_name, dst_file_name)
            os.system(cmd)
        else:
            dst_file_name = file_name
        class_ = _get_action_class(action)
        # We want to run the stages, and not check.
        output_tmp = class_.execute(dst_file_name, pedantic)
        # Annotate with executable [tag].
        output_tmp = _annotate_output(output_tmp, action)
        prnt.dassert_list_of_strings(
            output_tmp, "action=%s file_name=%s", action, file_name
        )
        output.extend(output_tmp)
        if output_tmp:
            _LOG.info("\n%s", "\n".join(output_tmp))
    return output


def _run_linter(
    actions: List[str], args: argparse.Namespace, file_names: List[str]
) -> List[str]:
    num_steps = len(file_names) * len(actions)
    _LOG.info(
        "Num of files=%d, num of actions=%d -> num of steps=%d",
        len(file_names),
        len(actions),
        num_steps,
    )
    pedantic = args.pedantic
    num_threads = args.num_threads
    ## Use serial mode if there is a single file, unless the user specified
    ## explicitly the numer of threads to use.
    if len(file_names) == 1 and num_threads != -1:
        num_threads = "serial"
        _LOG.warning(
            "Using num_threads='%s' since there is a single file", num_threads
        )
    output: List[str] = []
    if num_threads == "serial":
        for file_name in file_names:
            output_tmp = _lint(file_name, actions, pedantic, args.debug)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info(
            "Using %s threads", num_threads if num_threads > 0 else "all CPUs"
        )
        import joblib

        output_tmp = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_lint)(file_name, actions, pedantic, args.debug)
            for file_name in file_names
        )
        output_tmp = list(itertools.chain.from_iterable(output_tmp))
    output.extend(output_tmp)
    output = prnt.remove_empty_lines_from_string_list(output)
    return output  # type: ignore


def _count_lints(lints: List[str]) -> int:
    num_lints = 0
    for line in lints:
        # Example: 'dev_scripts/linter.py:493: ... [pydocstyle]'
        if re.match(r"\S+:\d+.*\[\S+\]", line):
            num_lints += 1
    _LOG.info("num_lints=%d", num_lints)
    return num_lints


# #############################################################################
# Main.
# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Select files.
    parser.add_argument(
        "-f", "--files", nargs="+", type=str, help="Files to process"
    )
    parser.add_argument(
        "-c",
        "--current_git_files",
        action="store_true",
        help="Select files modified in the current git client",
    )
    parser.add_argument(
        "-p",
        "--previous_git_committed_files",
        nargs="?",
        type=int,
        const=1,
        default=None,
        help="Select files modified in previous 'n' user git commit",
    )
    parser.add_argument(
        "-b",
        "--modified_files_in_branch",
        action="store_true",
        help="Select files modified in current branch with respect to master",
    )
    parser.add_argument(
        "-t",
        "--target_branch",
        action="store",
        type=str,
        help="Select files modified in current branch with respect to `target_branch`.",
    )
    parser.add_argument(
        "-d",
        "--dir_name",
        action="store",
        help="Select all files in a dir. 'GIT_ROOT' to select git root",
    )
    parser.add_argument(
        "--skip_files",
        action="append",
        help="Force skipping certain files, e.g., together with -d",
    )
    # Select files based on type.
    parser.add_argument(
        "--skip_py", action="store_true", help="Do not process python scripts"
    )
    parser.add_argument(
        "--skip_ipynb",
        action="store_true",
        help="Do not process jupyter notebooks",
    )
    parser.add_argument(
        "--skip_paired_jupytext",
        action="store_true",
        help="Do not process paired notebooks",
    )
    parser.add_argument(
        "--only_py",
        action="store_true",
        help="Process only python scripts excluding paired notebooks",
    )
    parser.add_argument(
        "--only_ipynb", action="store_true", help="Process only jupyter notebooks"
    )
    parser.add_argument(
        "--only_paired_jupytext",
        action="store_true",
        help="Process only paired notebooks",
    )
    # Debug.
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Generate one file per transformation",
    )
    parser.add_argument(
        "--no_cleanup", action="store_true", help="Do not clean up tmp files"
    )
    parser.add_argument("--jenkins", action="store_true", help="Run as jenkins")
    # Test.
    parser.add_argument(
        "--collect_only",
        action="store_true",
        help="Print only the files to process and stop",
    )
    parser.add_argument(
        "--test_actions", action="store_true", help="Print the possible actions"
    )
    # Select actions.
    prsr.add_action_arg(parser, _get_valid_actions(), _get_default_actions())
    #
    parser.add_argument(
        "--pedantic",
        action="store",
        type=int,
        default=0,
        help="Pedantic level. 0 = min, 2 = max (all the lints)",
    )
    parser.add_argument(
        "--num_threads",
        action="store",
        default="-1",
        help="Number of threads to use ('serial' to run serially, -1 to use "
        "all CPUs)",
    )
    #
    parser.add_argument(
        "--linter_log",
        default="./linter_warnings.txt",
        help="File storing the warnings",
    )
    parser.add_argument("--no_print", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(args: argparse.Namespace) -> int:
    dbg.init_logger(args.log_level)
    #
    if args.test_actions:
        _LOG.warning("Testing actions...")
        _test_actions()
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    if args.no_print:
        global NO_PRINT
        NO_PRINT = True
    # Get all the files to process.
    all_file_names = _get_files(args)
    _LOG.info("# Found %d files to process", len(all_file_names))
    # Select files.
    file_names = _get_files_to_lint(args, all_file_names)
    _LOG.info(
        "\n%s\n%s",
        prnt.frame("# Found %d files to lint:" % len(file_names)),
        prnt.space("\n".join(file_names)),
    )
    if args.collect_only:
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    # Select actions.
    actions = _select_actions(args)
    all_actions = actions[:]
    _LOG.debug("actions=%s", actions)
    # Create tmp dir.
    io_.create_dir(_TMP_DIR, incremental=False)
    _LOG.info("tmp_dir='%s'", _TMP_DIR)
    # Check the files.
    lints: List[str] = []
    lints_tmp, actions = _check_file_property(
        actions, all_file_names, args.pedantic
    )
    lints.extend(lints_tmp)
    # Run linter.
    lints_tmp = _run_linter(actions, args, file_names)
    prnt.dassert_list_of_strings(lints_tmp)
    lints.extend(lints_tmp)
    # Sort the errors.
    lints = sorted(lints)
    lints = hlist.remove_duplicates(lints)
    # Count number of lints.
    num_lints = _count_lints(lints)
    #
    output: List[str] = []
    output.append("cmd line='%s'" % dbg.get_command_line())
    # TODO(gp): datetime_.get_timestamp().
    ## output.insert(1, "datetime='%s'" % datetime.datetime.now())
    output.append("actions=%d %s" % (len(all_actions), all_actions))
    output.append("file_names=%d %s" % (len(file_names), file_names))
    output.extend(lints)
    output.append("num_lints=%d" % num_lints)
    # Write the file.
    output_as_str = "\n".join(output)
    io_.to_file(args.linter_log, output_as_str)
    # Print linter output.
    txt = io_.from_file(args.linter_log)
    _print(prnt.frame(args.linter_log, char1="/").rstrip("\n"))
    _print(txt + "\n")
    _print(prnt.line(char="/").rstrip("\n"))
    #
    if num_lints != 0:
        _LOG.warning(
            "You can quickfix the issues with\n> vim -c 'cfile %s'",
            args.linter_log,
        )
    #
    if not args.no_cleanup:
        io_.delete_dir(_TMP_DIR)
    else:
        _LOG.warning("Leaving tmp files in '%s'", _TMP_DIR)
    if args.jenkins:
        _LOG.warning("Skipping returning an error because of --jenkins")
        num_lints = 0
    return num_lints


if __name__ == "__main__":
    parser_ = _parse()
    args_ = parser_.parse_args()
    rc_ = _main(args_)
    sys.exit(rc_)
