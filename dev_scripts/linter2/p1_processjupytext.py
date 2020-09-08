#!/usr/bin/env python
r"""Wrapper for process_jupytext

> p1_process_jupytext.py sample_file1.py sample_file2.py
"""
import argparse
import logging
from typing import List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################


class _ProcessJupytext(lntr.Action):
    def __init__(self, jupytext_action: str) -> None:
        executable = "process_jupytext.py"
        super().__init__(executable)
        self._jupytext_action = jupytext_action

    def check_if_possible(self) -> bool:
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        output: List[str] = []
        # TODO(gp): Use the usual idiom of these functions.
        if utils.is_py_file(file_name) and utils.is_paired_jupytext_file(
            file_name
        ):
            cmd_opts = "-f %s --action %s" % (file_name, self._jupytext_action)
            cmd = self._executable + " " + cmd_opts
            rc, output = lntr.tee(cmd, self._executable, abort_on_error=False)
            if rc != 0:
                error = "process_jupytext failed with command `%s`\n" % cmd
                output.append(error)
                _LOG.error(output)
        else:
            _LOG.debug("Skipping file_name='%s'", file_name)
        return output


class _SyncJupytext(_ProcessJupytext):
    def __init__(self) -> None:
        super().__init__("sync")


class _TestJupytext(_ProcessJupytext):
    def __init__(self) -> None:
        super().__init__("test")


class _JupytextAction:
    @staticmethod
    def _execute(file_name: str, pedantic: int) -> List[str]:
        """Execute both JupyText actions in an interface similar to other
        actions, so we can use `lntr.run_action`"""
        actions: List[lntr.Action] = [_SyncJupytext, _TestJupytext]
        output: List[str] = []
        for action in actions:
            output.extend(
                action()._execute(  # pylint: disable=protected-access; temporary
                    file_name, pedantic
                )
            )
        return output


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _JupytextAction()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
