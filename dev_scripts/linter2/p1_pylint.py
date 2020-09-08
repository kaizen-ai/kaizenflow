#!/usr/bin/env python
r"""Wrapper for black

> p1_pylint.py sample_file1.py sample_file2.py
"""
import argparse
import logging
import re
from typing import List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################


class _Pylint(lntr.Action):
    def __init__(self) -> None:
        executable = "pylint"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        is_test_code_tmp = utils.is_under_test_dir(file_name)
        _LOG.debug("is_test_code_tmp=%s", is_test_code_tmp)
        #
        is_jupytext_code = utils.is_paired_jupytext_file(file_name)
        _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
        #
        opts = []
        ignore = []
        if pedantic < 2:
            # We ignore these errors as too picky.
            ignore.extend(
                [
                    # [C0302(too-many-lines), ] Too many lines in module (/1000)
                    "C0302",
                    # TODO(gp): Re-enable?
                    # [C0304(missing-final-newline), ] Final newline missing.
                    "C0304",
                    ## [C0330(bad-continuation), ] Wrong hanging indentation before
                    ##   block (add 4 spaces).
                    # - Black and pylint don't agree on the formatting.
                    "C0330",
                    ## [C0412(ungrouped-imports), ] Imports from package ... are not
                    ##   grouped.
                    # TODO(gp): Re-enable?
                    "C0412",
                    # [C0415(import-outside-toplevel), ] Import outside toplevel.
                    # - Sometimes we import inside a function.
                    "C0415",
                    # [R0902(too-many-instance-attributes)] Too many instance attributes (/7)
                    "R0902",
                    # [R0903(too-few-public-methods), ] Too few public methods (/2)
                    "R0903",
                    # [R0904(too-many-public-methods), ] Too many public methods (/20)
                    "R0904",
                    # [R0912(too-many-branches), ] Too many branches (/12)
                    "R0912",
                    # R0913(too-many-arguments), ] Too many arguments (/5)
                    "R0913",
                    # [R0914(too-many-locals), ] Too many local variables (/15)
                    "R0914",
                    # [R0915(too-many-statements), ] Too many statements (/50)
                    "R0915",
                    # [W0123(eval-used), ] Use of eval.
                    # - We assume that we are mature enough to use `eval` properly.
                    "W0123",
                    ## [W0125(using-constant-test), ] Using a conditional statement
                    ##   with a constant value:
                    # - E.g., we use sometimes `if True:` or `if False:`.
                    "W0125",
                    # [W0201(attribute-defined-outside-init)]
                    ## - If the constructor calls a method (e.g., `reset()`) to
                    ##   initialize the state, we have all these errors.
                    "W0201",
                    # [W0511(fixme), ]
                    "W0511",
                    # [W0603(global-statement), ] Using the global statement.
                    # - We assume that we are mature enough to use `global` properly.
                    "W0603",
                    # [W0640(cell-var-from-loop): Cell variable  defined in loop.
                    "W0640",
                    ## [W1113(keyword-arg-before-vararg), ] Keyword argument before variable
                    ##   positional arguments list in the definition of.
                    # - TODO(gp): Not clear what is the problem.
                    "W1113",
                ]
            )
            # Unit test.
            if is_test_code_tmp:
                ignore.extend(
                    [
                        ## [C0103(invalid-name), ] Class name "Test_dassert_eq1"
                        ##   doesn't conform to PascalCase naming style.
                        "C0103",
                        # [R0201(no-self-use), ] Method could be a function.
                        "R0201",
                        ## [W0212(protected-access), ] Access to a protected member
                        ##   _get_default_tempdir of a client class.
                        "W0212",
                    ]
                )
            # Jupytext.
            if is_jupytext_code:
                ignore.extend(
                    [
                        ## [W0104(pointless-statement), ] Statement seems to
                        ## have no effect.
                        ## - This is disabled since we use just variable names
                        ##   to print.
                        "W0104",
                        ## [W0106(expression-not-assigned), ] Expression ... is
                        ## assigned to nothing.
                        "W0106",
                        ## [W0621(redefined-outer-name), ] Redefining name ...
                        ## from outer scope.
                        "W0621",
                    ]
                )
        if pedantic < 1:
            ignore.extend(
                [
                    ## [C0103(invalid-name), ] Constant name "..." doesn't
                    ##   conform to UPPER_CASE naming style.
                    "C0103",
                    # [C0111(missing - docstring), ] Missing module docstring.
                    "C0111",
                    # [C0301(line-too-long), ] Line too long (/100)
                    # "C0301",
                ]
            )
        if ignore:
            opts.append("--disable " + ",".join(ignore))
        # Allow short variables, as long as they are camel-case.
        opts.append('--variable-rgx="[a-z0-9_]{1,30}$"')
        opts.append('--argument-rgx="[a-z0-9_]{1,30}$"')
        # TODO(gp): Not sure this is needed anymore.
        opts.append("--ignored-modules=pandas")
        opts.append("--output-format=parseable")
        # TODO(gp): Does -j 4 help?
        opts.append("-j 4")
        # pylint crashed due to lack of memory.
        # A fix according to https://github.com/PyCQA/pylint/issues/2388 is:
        opts.append('--init-hook="import sys; sys.setrecursionlimit(2000)"')
        dbg.dassert_list_of_strings(opts)
        opts_as_str = " ".join(opts)
        cmd = " ".join([self._executable, opts_as_str, file_name])
        _, output = lntr.tee(cmd, self._executable, abort_on_error=False)
        # Remove some errors.
        output_tmp: List[str] = []
        for line in output:
            if is_jupytext_code:
                # [E0602(undefined-variable), ] Undefined variable 'display'
                if "E0602" in line and "Undefined variable 'display'" in line:
                    continue
            if line.startswith("Your code has been rated"):
                # Your code has been rated at 10.00/10 (previous run: ...
                continue
            output_tmp.append(line)
        output = output_tmp
        # Remove lines.
        output = [line for line in output if ("-" * 20) not in line]
        # Remove:
        #    ************* Module dev_scripts.generate_script_catalog.
        output_as_str = ut.filter_text(
            re.escape("^************* Module "), "\n".join(output)
        )
        # Remove empty lines.
        output = output_as_str.split("\n")
        output = [line for line in output if line.rstrip().lstrip() != ""]
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
    action = _Pylint()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
