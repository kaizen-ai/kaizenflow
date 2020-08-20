import logging
import os
from typing import Tuple

import pytest

import dev_scripts.linter.base as lntr
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################
## linter.py
# #############################################################################


# pylint: disable=too-many-public-methods
@pytest.mark.amp
@pytest.mark.skip(reason="Disabled because of AmpTask508")
class Test_linter_py1(ut.TestCase):
    def _write_input_file(self, txt: str, file_name: str) -> Tuple[str, str]:
        dir_name = self.get_scratch_space()
        dbg.dassert_is_not(file_name, None)
        file_name = os.path.join(dir_name, file_name)
        file_name = os.path.abspath(file_name)
        io_.to_file(file_name, txt)
        return dir_name, file_name

    def _run_linter(
        self, file_name: str, linter_log: str, as_system_call: bool,
    ) -> str:
        if as_system_call:
            cmd = []
            cmd.append(f"linter.py -f {file_name} --linter_log {linter_log}")
            cmd_as_str = " ".join(cmd)
            ## We need to ignore the errors reported by the script, since it
            ## represents how many lints were found.
            suppress_output = _LOG.getEffectiveLevel() > logging.DEBUG
            si.system(
                cmd_as_str, abort_on_error=False, suppress_output=suppress_output
            )
        else:
            logger_verbosity = dbg.get_logger_verbosity()
            parser = lntr._parse()
            args = parser.parse_args(
                [
                    "-f",
                    file_name,
                    "--linter_log",
                    linter_log,
                    # TODO(gp): Avoid to call the logger.
                    "-v",
                    "ERROR",
                    # No output from the print.
                    "--no_print",
                ]
            )
            lntr._main(args)
            dbg.init_logger(logger_verbosity)

        # Read log.
        _LOG.debug("linter_log=%s", linter_log)
        txt = io_.from_file(linter_log)
        # Process log.
        output = []
        output.append("# linter log")
        for line in txt.split("\n"):
            # Remove the line:
            #   Cmd line='.../linter.py -f input.py --linter_log ./linter.log'
            if "cmd line=" in line:
                continue
            # Filter out code rate because of #2241.
            if "Your code has been rated at" in line:
                continue
            output.append(line)
        # Read output.
        _LOG.debug("file_name=%s", file_name)
        output.append("# linter file")
        txt = io_.from_file(file_name)
        output.extend(txt.split("\n"))
        #
        output_as_str = "\n".join(output)
        return output_as_str

    def _helper(self, txt: str, file_name: str, as_system_call: bool) -> str:
        # Create file to lint.
        dir_name, file_name = self._write_input_file(txt, file_name)
        # Run.
        dir_name = self.get_scratch_space()
        linter_log = "linter.log"
        linter_log = os.path.abspath(os.path.join(dir_name, linter_log))
        output = self._run_linter(file_name, linter_log, as_system_call)
        return output

    # #########################################################################

    @staticmethod
    def _get_horrible_python_code1() -> str:
        txt = r"""
import python


if __name__ == "main":
    txt = "hello"
    m = re.search("\s", txt)
        """
        return txt

    @pytest.mark.skip(reason="Disable because of PartTask3409")
    @pytest.mark.skipif(
        'si.get_server_name() == "docker-instance"', reason="Issue #1522, #1831"
    )
    def test_linter1(self) -> None:
        """Run linter.py as executable on some text."""
        txt = self._get_horrible_python_code1()
        # Run.
        file_name = "input.py"
        as_system_call = True
        output = self._helper(txt, file_name, as_system_call)
        # Check.
        self.check_string(output, purify_text=True)

    @pytest.mark.skip(reason="Disable because of PartTask3409")
    @pytest.mark.skipif(
        'si.get_server_name() == "docker-instance"', reason="Issue #1522, #1831"
    )
    def test_linter2(self) -> None:
        """Run linter.py as library on some text."""
        txt = self._get_horrible_python_code1()
        # Run.
        file_name = "input.py"
        as_system_call = False
        output = self._helper(txt, file_name, as_system_call)
        # Check.
        self.check_string(output, purify_text=True)

    @pytest.mark.skip(reason="Disabled until #2430 is solved")
    def test_linter_md1(self) -> None:
        """Run linter.py as executable on some text."""
        txt = r"""
# Good.
- Good time management
  1. choose the right tasks
    - Avoid non-essential tasks

## Bad
-  Hello
    - World
        """
        # Run.
        file_name = "hello.md"
        as_system_call = True
        output = self._helper(txt, file_name, as_system_call)
        # Remove the line:
        # '12-16_14:59 ^[[33mWARNING^[[0m: _refresh_toc   :138 : No tags for table'
        output = ut.filter_text("No tags for table", output)
        # Check.
        self.check_string(output)
