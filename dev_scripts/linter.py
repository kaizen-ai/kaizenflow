#!/usr/bin/env python
"""
Reformat and lint python and ipynb files.

This script uses the version of the files present on the disk and not what is
staged for commit by git, thus you need to stage again after running it.

E.g.,
# Lint all modified files in git client.
> linter.py

# Lint current files.
> linter.py --current_git_files --collect_only
> linter.py --current_git_files --all

# Lint previous commit files.
> linter.py --previous_git_commit_files --collect_only

# Lint a certain number of previous commits
> linter.py --previous_git_commit_files n --collect_only
> linter.py --files event_study/*.py linter_v2.py --yapf --isort -v DEBUG

# Lint all python files, but not the notebooks.
> linter.py -d . --only_py --collect

# To jump to all the warnings to fix:
> vim -c "cfile linter.log"

# Check all jupytext files.
> linter.py -d . --action sync_jupytext
"""

import argparse
import itertools
import logging
import os
import py_compile
import re
import sys
from typing import Any, List, Tuple

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# Use the current dir and not the dir of the executable.
_TMP_DIR = os.path.abspath(os.getcwd() + "/tmp.linter")


# #############################################################################
# Utils.
# #############################################################################


# TODO(gp): This could become the default behavior of system().
def _system(cmd: str, abort_on_error: bool = True) -> int:
    suppress_output = _LOG.getEffectiveLevel() > logging.DEBUG
    rc = si.system(
        cmd,
        abort_on_error=abort_on_error,
        suppress_output=suppress_output,
        log_level=logging.DEBUG,
    )
    return rc


def _remove_empty_lines(output: List[str]) -> List[str]:
    output = [l for l in output if l.strip("\n") != ""]
    return output


def _dassert_list_of_strings(output: List[str], *args: Any) -> None:
    dbg.dassert_isinstance(output, list, *args)
    for line in output:
        dbg.dassert_isinstance(line, str, *args)


# TODO(gp): Horrible: to remove / rewrite.
def _clean_file(file_name, write_back):
    """
    Remove empty spaces, tabs, windows end-of-lines.
    :param write_back: if True the file is overwritten in place.
    """
    # Read file.
    file_in: List[str] = []
    # TODO(gp): Use io_.from_file
    with open(file_name, "r") as f:
        for line in f:
            file_in.append(line)
    #
    file_out: List[str] = []
    for line in file_in:
        # A line can be deleted if it has only spaces and \n.
        if not any(char not in (" ", "\n") for char in line):
            line = "\n"
        # dos2unix.
        line = line.replace("\r\n", "\n")
        file_out.append(line)
    # Remove whitespaces at the end of file.
    while file_out and (file_out[-1] == "\n"):
        # While the last item in the list is blank, removes last element.
        file_out.pop(-1)
    # Write the new the output to file.
    if write_back:
        # TODO(gp): Use _write_back.
        file_in = "".join(file_in)
        file_out = "".join(file_out)
        if file_in != file_out:
            _LOG.debug("Writing back file '%s'", file_name)
            with open(file_name, "w") as f:
                f.write(file_out)
        else:
            _LOG.debug("No change in file, so no saving")
    return file_in, file_out


def _annotate_output(output: List, executable: str) -> List:
    """
    Annotate a list containing the output of a cmd line with the name of the
    executable used.
    :return: list of strings
    """
    _dassert_list_of_strings(output)
    output = [t + " [%s]" % executable for t in output]
    _dassert_list_of_strings(output)
    return output


def _tee(cmd: str, executable: str, abort_on_error: bool) -> List[str]:
    """
    Execute command "cmd", capturing its output and removing empty lines.
    :return: list of strings
    """
    _LOG.debug("cmd=%s executable=%s", cmd, executable)
    _, output = si.system_to_string(cmd, abort_on_error=abort_on_error)
    dbg.dassert_isinstance(output, str)
    _LOG.debug("output1=\n'%s'", output)
    #
    output2 = _remove_empty_lines(output.split("\n"))
    _LOG.debug("output2=\n'%s'", "\n".join(output2))
    _dassert_list_of_strings(output2)
    return output2


# TODO(gp): Move to system_interactions.
def _check_exec(tool: str) -> bool:
    """
    :return: True if the executables "tool" can be executed.
    """
    rc = _system("which %s" % tool, abort_on_error=False)
    return rc == 0


# #############################################################################
# Handle files.
# #############################################################################


def _filter_target_files(file_names: List[str]) -> List[str]:
    """
    Keep only the files that:
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
        # Skip tmp names since we need to run on unit tests.
        if False:
            # Skip files in directory starting with "tmp.".
            is_valid &= "/tmp." not in file_name
            # Skip files starting with "tmp.".
            is_valid &= not file_name.startswith("tmp.")
        if is_valid:
            file_names_out.append(file_name)
    return file_names_out


def _get_files(args) -> List[str]:
    """
    Return the list of files to process given the command line arguments.
    """
    file_names: List[str] = []
    if args.files:
        _LOG.debug("Specified files")
        # User has specified files.
        file_names = args.files
    else:
        if args.previous_git_commit_files is not None:
            _LOG.debug("Looking for files committed in previous Git commit")
            # Get all the git in user previous commit.
            n_commits = args.previous_git_commit_files
            _LOG.info("Using %s previous commits", n_commits)
            file_names = git.get_previous_committed_files(n_commits)
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
        if not file_names or args.current_git_files:
            # Get all the git modified files.
            file_names = git.get_modified_files()
    # Remove text files used in unit tests.
    file_names = [f for f in file_names if not is_test_input_output_file(f)]
    # Make all paths absolute.
    # file_names = [os.path.abspath(f) for f in file_names]
    # Check files exist.
    for f in file_names:
        dbg.dassert_exists(f)
    return file_names


def _get_files_to_lint(args, file_names: List[str]) -> List[str]:
    """
    Get all the files that need to be linted.

    Typically files to lint are python and notebooks.
    """
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    # Keep only actual .py and .ipynb files.
    file_names = _filter_target_files(file_names)
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    # Remove files.
    if args.skip_py:
        file_names = [f for f in file_names if not is_py_file(f)]
    if args.skip_ipynb:
        file_names = [f for f in file_names if not is_ipynb_file(f)]
    if args.skip_paired_jupytext:
        file_names = [f for f in file_names if not is_paired_jupytext_file(f)]
    # Keep files.
    if args.only_py:
        file_names = [
            f
            for f in file_names
            if is_py_file(f) and not is_paired_jupytext_file(f)
        ]
    if args.only_ipynb:
        file_names = [f for f in file_names if is_ipynb_file(f)]
    if args.only_paired_jupytext:
        file_names = [f for f in file_names if is_paired_jupytext_file(f)]
    #
    _LOG.debug("file_names=(%s) %s", len(file_names), " ".join(file_names))
    if len(file_names) < 1:
        _LOG.warning("No files that can be linted are specified")
    return file_names


# ##############################################################################

# TODO(gp): We should use a Strategy pattern, having a base class and a class
#  for each action.

# Each action accepts:
# :param file_name: name of the file to process
# :param pendantic: True if it needs to be run in angry mode
# :param check_if_possible: check if the action can be executed on filename
# :return: list of strings representing the output


def _write_file_back(file_name: str, txt: List[str], txt_new: List[str]) -> None:
    _dassert_list_of_strings(txt)
    txt_as_str = "\n".join(txt)
    #
    _dassert_list_of_strings(txt_new)
    txt_new_as_str = "\n".join(txt_new)
    #
    if txt_as_str != txt_new_as_str:
        io_.to_file(file_name, txt_new_as_str)


# TODO(gp): joblib asserts when using abstract classes:
#   AttributeError: '_BasicHygiene' object has no attribute '_executable'
# class _Action(abc.ABC):
class _Action:
    def __init__(self, executable=None):
        self._executable = executable

    # @abc.abstractmethod
    def check_if_possible(self) -> bool:
        pass

    def execute(self, file_name: str, pedantic: bool) -> List[str]:
        dbg.dassert(file_name)
        dbg.dassert_exists(file_name)
        output = self._execute(file_name, pedantic)
        _dassert_list_of_strings(output)
        return output

    # @abc.abstractmethod
    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        pass


# ##############################################################################


class _CheckFileProperty(_Action):
    """
    Perform various checks based on property of a file:
    - check that file size doesn't exceed a certain threshould
    - check that notebook files are under a `notebooks` dir
    - check that test files are under `test` dir
    """

    def check_if_possible(self) -> bool:
        # We don't need any special executable, so we can always run this action.
        return True

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        output: List[str] = []
        for func in [
            self._check_size,
            self._check_notebook_dir,
            self._check_test_file_dir,
        ]:
            msg = func(file_name)
            if msg:
                _LOG.warning(msg)
                output.append(msg)
        return output

    @staticmethod
    def _check_size(file_name: str) -> str:
        """
        Check size of a file.
        """
        msg = ""
        max_size_in_bytes = 512 * 1024
        size_in_bytes = os.path.getsize(file_name)
        if size_in_bytes > max_size_in_bytes:
            msg = "%s:1: file size is too large %s > %s" % (
                file_name,
                size_in_bytes,
                max_size_in_bytes,
            )
        return msg

    @staticmethod
    def _check_notebook_dir(file_name: str) -> str:
        """
        # Check if that notebooks are under `notebooks` dir.
        """
        msg = ""
        if is_ipynb_file(file_name):
            subdir_names = file_name.split("/")
            if "notebooks" not in subdir_names:
                msg = (
                    "%s:1: each notebook should be under a 'notebooks' "
                    "directory to not confuse pytest" % file_name
                )
        return msg

    @staticmethod
    def _check_test_file_dir(file_name: str) -> str:
        """
        Check if test files are under `test` dir.
        """
        msg = ""
        # TODO(gp): A little annoying that we use "notebooks" and "test".
        if is_py_file(file_name) and os.path.basename(file_name).startswith(
            "test_"
        ):
            if not is_under_test_dir(file_name):
                msg = (
                    "%s:1: test files should be under 'test' directory to "
                    "be discovered by pytest" % file_name
                )
        return msg


# ##############################################################################


class _BasicHygiene(_Action):
    def check_if_possible(self) -> bool:
        # We don't need any special executable, so we can always run this action.
        return True

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        output: List[str] = []
        # Read file.
        txt = io_.from_file(file_name, split=True)
        # Process file.
        txt_new: List[str] = []
        for line in txt:
            if "\t" in line:
                msg = (
                    "Found tabs in %s: please use 4 spaces as per PEP8"
                    % file_name
                )
                _LOG.warning(msg)
                output.append(msg)
            # Convert tabs.
            line = line.replace("\t", " " * 4)
            # Remove trailing spaces.
            line = line.rstrip()
            # dos2unix.
            line = line.replace("\r\n", "\n")
            # TODO(gp): Remove empty lines in functions.
            txt_new.append(line.rstrip("\n"))
        # Remove whitespaces at the end of file.
        while txt_new and (txt_new[-1] == "\n"):
            # While the last item in the list is blank, removes last element.
            txt_new.pop(-1)
        # Write.
        _write_file_back(file_name, txt, txt_new)
        return output


class _CompilePython(_Action):
    """
    Check that the code is valid python.
    """

    def check_if_possible(self) -> bool:
        # We don't need any special executable, so we can always run this action.
        return True

    def _execute(self, file_name, pedantic: bool) -> List[str]:
        _ = pedantic
        output: List[str] = []
        # Applicable only to python files.
        if not is_py_file(file_name):
            _LOG.debug("Skipping self._file_name='%s'", file_name)
            return output
        #
        try:
            py_compile.compile(file_name, doraise=True)
            # pylint: disable=broad-except
        except Exception as e:
            output.append(str(e))
        return output


class _CustomPythonChecks(_Action):
    # The maximum length of an 'import as'.
    MAX_LEN_IMPORT = 5

    def check_if_possible(self) -> bool:
        # We don't need any special executable, so we can always run this action.
        return True

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        output: List[str] = []
        # Applicable only to python files.
        if not is_py_file and not is_paired_jupytext_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return output
        # Read file.
        txt = io_.from_file(file_name, split=True)
        # Check shebang.
        is_executable = os.access(file_name, os.X_OK)
        msg = self._check_shebang(file_name, txt, is_executable)
        if msg:
            output.append(msg)
        # Check that the module was baptized.
        msg = self._was_baptized(file_name, txt)
        if msg:
            output.append(msg)
        # Process file.
        output_tmp, txt_new = self._check_text(file_name, txt)
        output.extend(output_tmp)
        # Write file back.
        _write_file_back(file_name, txt, txt_new)
        return output

    @staticmethod
    def _check_shebang(
        file_name: str, txt: List[str], is_executable: bool
    ) -> str:
        msg = ""
        shebang = "#!/usr/bin/env python"
        has_shebang = txt[0] == shebang
        if (is_executable and not has_shebang) or (
            not is_executable and has_shebang
        ):
            msg = "%s:1: any executable needs to start with a shebang '%s'" % (
                file_name,
                shebang,
            )
        return msg

    @staticmethod
    def _check_import(file_name: str, line_num: int, line: str) -> str:
        msg = ""
        _LOG.debug("* Check 'from * imports'")
        m = re.match(r"\s*from\s+(\S+)\s+import\s+.*", line)
        if m:
            if m.group(1) != "typing":
                msg = "%s:%s: do not use '%s' use 'import foo.bar " "as fba'" % (
                    file_name,
                    line_num,
                    line,
                )
        else:
            m = re.match(r"\s*import\s+\S+\s+as\s+(\S+)", line)
            if m:
                shortcut = m.group(1)
                if len(shortcut) > _CustomPythonChecks.MAX_LEN_IMPORT:
                    msg = (
                        "%s:%s: the import shortcut '%s' in '%s' is longer than "
                        "%s characters"
                        % (
                            file_name,
                            line_num,
                            shortcut,
                            line,
                            _CustomPythonChecks.MAX_LEN_IMPORT,
                        )
                    )
        return msg

    @staticmethod
    def _was_baptized(file_name, txt: List[str]) -> str:
        """
        Check if code contains a declaration of how to be imported.
        """
        msg: List[str] = []
        # Check that the header of the file is in the format:
        #   """
        #   Import as:
        #
        #   import _setenv_lib as selib
        #   ...
        _dassert_list_of_strings(txt)
        if len(txt) > 3:
            match = True
            match &= txt[0] == '"""'
            match &= txt[1] == "Import as:"
            match &= txt[2] == ""
            match &= txt[3].startswith("import ")
        else:
            match = False
        if not match:
            msg.append(
                "%s:1: every library needs to describe how to be imported:"
                % file_name
            )
            msg.append('"""')
            msg.append("Import as:")
            msg.append("\nimport foo.bar as fba")
            msg.append('"""')
        else:
            # Check that the import is in the right format, like:
            #   import _setenv_lib as selib
            import_line = 3
            line = txt[import_line]
            _LOG.debug("import line=%s", line)
            # m = re.match(r"\s*import\s+\S+\s+as\s+(\S+)", txt[import_line])
            # if m:
            #     shortcut = m.group(1)
            #     if len(shortcut) > _CustomPythonChecks.MAX_LEN_IMPORT:
            #         msg.append(
            #             "%s:%s: the import shortcut '%s' is longer than "
            #             "%s characters"
            #             % (file_name, import_line, shortcut, max_len)
            #         )
            # else:
            #     msg.append(
            #         "%s:%s: the import is not in the right format "
            #         "'import foo.bar as fba'" % (file_name, import_line)
            #     )
            msg_tmp = _CustomPythonChecks._check_import(
                file_name, import_line, line
            )
            if msg_tmp:
                msg.append(msg_tmp)
        msg_as_str = "\n".join(msg)
        return msg_as_str

    @staticmethod
    def _check_text(
        file_name: str, txt: List[str]
    ) -> Tuple[List[str], List[str]]:
        _dassert_list_of_strings(txt)
        output: List[str] = []
        txt_new: List[str] = []
        for i, line in enumerate(txt):
            _LOG.debug("%s: line='%s'", i, line)
            # Check imports.
            _LOG.debug("* Check imports")
            # shortcut = m.group(1)
            # if len(shortcut) > max_len:
            #     msg.append(
            #         "%s:%s: the import shortcut '%s' is longer than "
            #         "%s characters"
            #         % (file_name, import_line, shortcut, max_len)
            #     )
            msg = _CustomPythonChecks._check_import(file_name, i + 1, line)
            if msg:
                output.append(msg)
            # Look for conflicts markers.
            _LOG.debug("* Look for conflict markers")
            if any(line.startswith(c) for c in ["<<<<<<<", "=======", ">>>>>>>"]):
                msg = "%s:%s: there are conflict markers" % (file_name, i + 1)
                output.append(msg)
            # Format separating lines.
            _LOG.debug("* Format separating lines")
            min_num_chars = 5
            for char in "# = - < >".split():
                regex = r"(\s*\#)\s*" + (("\\" + char) * min_num_chars)
                _LOG.debug("regex=%s", regex)
                m = re.match(regex, line)
                if m:
                    line = m.group(1) + " " + char * (80 - len(m.group(1)))
            #
            _LOG.debug("    -> %s", line)
            txt_new.append(line)
            _dassert_list_of_strings(txt_new)
            #
            _dassert_list_of_strings(output)
        return output, txt_new


class _Autoflake(_Action):
    """
    Remove unused imports and variables.
    """

    def __init__(self):
        executable = "autoflake"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        opts = "-i --remove-all-unused-imports --remove-unused-variables"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output = _tee(cmd, self._executable, abort_on_error=False)
        return output


class _Yapf(_Action):
    """
    Apply yapf code formatter.
    """

    def __init__(self):
        executable = "yapf"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        opts = "-i --style='google'"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output = _tee(cmd, self._executable, abort_on_error=False)
        return output


class _Black(_Action):
    """
    Apply black code formatter.
    """

    def __init__(self):
        executable = "black"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        opts = "--line-length 82"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output = _tee(cmd, self._executable, abort_on_error=False)
        # Remove the lines:
        # - reformatted core/test/test_core.py
        # - 1 file reformatted.
        # - All done!
        # - 1 file left unchanged.
        to_remove = ["All done!", "file left unchanged", "reformatted"]
        output = [l for l in output if all(w not in l for w in to_remove)]
        return output


class _Isort(_Action):
    """
    Sort imports using isort.
    """

    def __init__(self):
        executable = "isort"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        cmd = self._executable + " %s" % file_name
        output = _tee(cmd, self._executable, abort_on_error=False)
        return output


class _Flake8(_Action):
    """
    Look for formatting and semantic issues in code and docstrings.
    It relies on:
        - mccabe
        - pycodestyle
        - pyflakes
    """

    def __init__(self):
        executable = "flake8"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        # TODO(gp): Does -j 4 help?
        opts = "--exit-zero --doctests --max-line-length=82 -j 4"
        ignore = [
            # - "W503 line break before binary operator"
            #     - Disabled because in contrast with black formatting.
            "W503",
            # - W504 line break after binary operator
            #     - Disabled because in contrast with black formatting.
            "W504",
            # - E203 whitespace before ':'
            #     - Disabled because in contrast with black formatting
            "E203",
            # - E266 too many leading '#' for block comment
            #     - We have disabled this since it is a false positive for jupytext
            #       files.
            "E266,"
            # - E501 line too long (> 82 characters)
            #     - We have disabled this since it triggers also for docstrings at
            #       the beginning of the line. We let pylint pick the lines too
            #       long, since it seems to be smarter.
            "E501",
            # - E731 do not assign a lambda expression, use a def
            "E731",
            # - E265 block comment should start with '# '
            "E265",
        ]
        is_jupytext_code = is_paired_jupytext_file(file_name)
        _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
        if is_jupytext_code:
            ignore.extend(
                [
                    # E501 line too long.
                    "E501"
                ]
            )
        opts += " --ignore=" + ",".join(ignore)
        cmd = self._executable + " %s %s" % (opts, file_name)
        #
        output = _tee(cmd, self._executable, abort_on_error=True)
        # Remove some errors.
        is_jupytext_code = is_paired_jupytext_file(file_name)
        _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
        if is_jupytext_code:
            output_tmp: List[str] = []
            for line in output:
                # F821 undefined name 'display' [flake8]
                if "F821" in line and "undefined name 'display'" in line:
                    continue
                output_tmp.append(line)
            output = output_tmp
        return output


class _Pydocstyle(_Action):
    def __init__(self):
        executable = "pydocstyle"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        # http://www.pydocstyle.org/en/2.1.1/error_codes.html
        ignore = [
            # D105: Missing docstring in magic method
            "D105",
            # D200: One-line docstring should fit on one line with quotes
            "D200",
            # D202: No blank lines allowed after function docstring
            "D202",
            # D212: Multi-line docstring summary should start at the first line
            "D212",
            # D203: 1 blank line required before class docstring (found 0)
            "D203",
            # D205: 1 blank line required between summary line and description
            "D205",
            # D400: First line should end with a period (not ':')
            "D400",
            # D402: First line should not be the function's "signature"
            "D402",
            # D407: Missing dashed underline after section
            "D407",
            # D413: Missing dashed underline after section
            "D413",
            # D415: First line should end with a period, question mark, or
            # exclamation point
            "D415",
        ]
        if not pedantic:
            ignore.extend(
                [
                    # D100: Missing docstring in public module
                    "D100",
                    # D101: Missing docstring in public class
                    "D101",
                    # D102: Missing docstring in public method
                    "D102",
                    # D103: Missing docstring in public function
                    "D103",
                    # D104: Missing docstring in public package
                    "D104",
                    # D107: Missing docstring in __init__
                    "D107",
                ]
            )
        opts = ""
        if ignore:
            opts += "--ignore " + ",".join(ignore)
        # yapf: disable
        cmd = self._executable + " %s %s" % (opts, file_name)
        # yapf: enable
        # We don't abort on error on pydocstyle, since it returns error if there is
        # any violation.
        _, file_lines_as_str = si.system_to_string(cmd, abort_on_error=False)
        # Process lint_log transforming:
        #   linter_v2.py:1 at module level:
        #       D400: First line should end with a period (not ':')
        # into:
        #   linter_v2.py:1: at module level: D400: First line should end with a
        #   period (not ':')
        #
        output: List[str] = []
        #
        file_lines = file_lines_as_str.split("\n")
        lines = ["", ""]
        for cnt, line in enumerate(file_lines):
            line = line.rstrip("\n")
            # _log.debug("line=%s", line)
            if cnt % 2 == 0:
                regex = r"(\s(at|in)\s)"
                subst = r":\1"
                line = re.sub(regex, subst, line)
            else:
                line = line.lstrip()
            # _log.debug("-> line=%s", line)
            lines[cnt % 2] = line
            if cnt % 2 == 1:
                line = "".join(lines)
                output.append(line)
        return output


class _Pyment(_Action):
    def __init__(self):
        executable = "pyment"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        opts = "-w --first-line False -o reST"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output = _tee(cmd, self._executable, abort_on_error=False)
        return output


class _Pylint(_Action):
    def __init__(self):
        executable = "pylint"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        # Applicable to only python file.
        if not is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        opts = ""
        # We ignore these errors as too picky.
        ignore = [
            # [C0302(too-many-lines), ] Too many lines in module (/1000)
            "C0302",
            # [C0304(missing-final-newline), ] Final newline missing
            "C0304",
            # [C0330(bad-continuation), ] Wrong hanging indentation before block
            #   (add 4 spaces).
            # - Black and pylint don't agree on the formatting.
            "C0330",
            # [C0412(ungrouped-imports), ] Imports from package ... are not grouped
            "C0412",
            # [C0415(import-outside-toplevel), ] Import outside toplevel
            "C0415",
            # [R0903(too-few-public-methods), ] Too few public methods (/2)
            "R0903",
            # [R0912(too-many-branches), ] Too many branches (/12)
            "R0912",
            # R0913(too-many-arguments), ] Too many arguments (/5)
            "R0913",
            # [R0914(too-many-locals), ] Too many local variables (/15)
            "R0914",
            # [R0915(too-many-statements), ] Too many statements (/50)
            "R0915",
            # [W0123(eval-used), ] Use of eval
            "W0123",
            # [W0125(using-constant-test), ] Using a conditional statement with a
            #   constant value
            "W0125",
            # [W0511(fixme), ]
            "W0511",
            # [W0603(global-statement), ] Using the global statement
            # - We assume that we are mature enough to use `global` properly.
            "W0603",
            # [W1113(keyword-arg-before-vararg), ] Keyword argument before variable
            #   positional arguments list in the definition of
            # - TODO(gp): Not clear what is the problem.
            "W1113",
        ]
        is_test_code_tmp = is_under_test_dir(file_name)
        _LOG.debug("is_test_code_tmp=%s", is_test_code_tmp)
        if is_test_code_tmp:
            # TODO(gp): For files inside "test", disable:
            ignore.extend(
                [
                    # [C0103(invalid-name), ] Class name "Test_dassert_eq1"
                    #   doesn't conform to PascalCase naming style
                    "C0103",
                    # [R0201(no-self-use), ] Method could be a function
                    "R0201",
                    # [W0212(protected-access), ] Access to a protected member
                    #   _get_default_tempdir of a client class
                    "W0212",
                ]
            )
        is_jupytext_code = is_paired_jupytext_file(file_name)
        _LOG.debug("is_jupytext_code=%s", is_jupytext_code)
        if is_jupytext_code:
            ignore.extend(
                [
                    # [W0104(pointless-statement), ] Statement seems to have no effect
                    # This is disabled since we use just variable names to print.
                    "W0104",
                    # [W0106(expression-not-assigned), ] Expression # ... is
                    # assigned to nothing
                    "W0106",
                    # [W0621(redefined-outer-name), ] Redefining name ... from outer
                    # scope
                    "W0621",
                ]
            )
        if not pedantic:
            ignore.extend(
                [
                    # [C0103(invalid-name), ] Constant name "..." doesn't conform to
                    #   UPPER_CASE naming style
                    "C0103",
                    # [C0111(missing - docstring), ] Missing module docstring
                    "C0111",
                    # [C0301(line-too-long), ] Line too long (1065/100)
                    "C0301",
                ]
            )
        if ignore:
            opts += "--disable " + ",".join(ignore)
        # Allow short variables, as long as camel-case.
        opts += ' --variable-rgx="[a-z0-9_]{1,30}$"'
        opts += ' --argument-rgx="[a-z0-9_]{1,30}$"'
        # TODO(gp): Not sure this is needed anymore.
        opts += " --ignored-modules=pandas"
        opts += " --output-format=parseable"
        # TODO(gp): Does -j 4 help?
        opts += " -j 4"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output = _tee(cmd, self._executable, abort_on_error=False)
        # Remove some errors.
        output_tmp: List[str] = []
        for line in output:
            if is_jupytext_code:
                # [E0602(undefined-variable), ] Undefined variable 'display'
                if "E0602" in line and "Undefined variable 'display'" in line:
                    continue
            if line.startswith("Your code has been rated"):
                # Your code has been rated at 10.00/10 (previous run: ...
                line = file_name + ": " + line
            output_tmp.append(line)
        output = output_tmp
        # Remove lines.
        output = [l for l in output if ("-" * 20) not in l]
        return output


class _Mypy(_Action):
    def __init__(self):
        executable = "mypy"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        # Applicable to only python files, that are not paired with notebooks.
        if not is_py_file(file_name) or is_paired_jupytext_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        cmd = self._executable + " %s" % file_name
        _system(
            cmd,
            # mypy returns -1 if there are errors.
            abort_on_error=False,
        )
        output = _tee(cmd, self._executable, abort_on_error=False)
        # Remove some errors.
        output_tmp: List[str] = []
        for line in output:
            if (
                line.startswith("Success:")
                or
                # Found 2 errors in 1 file (checked 1 source file)
                line.startswith("Found ")
                or
                # note: See https://mypy.readthedocs.io
                "note: See https" in line
            ):
                continue
            output_tmp.append(line)
        output = output_tmp
        return output


# ##############################################################################


class _IpynbFormat(_Action):
    def __init__(self):
        curr_path = os.path.dirname(os.path.realpath(sys.argv[0]))
        executable = "%s/ipynb_format.py" % curr_path
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        output: List[str] = []
        # Applicable to only ipynb file.
        if not is_ipynb_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return output
        #
        cmd = self._executable + " %s" % file_name
        _system(cmd)
        return output


# TODO(gp): Move in a more general file.
def _is_under_dir(file_name: str, dir_name: str) -> bool:
    """
    Return whether a file is under the given directory.
    """
    subdir_names = file_name.split("/")
    return dir_name in subdir_names


def is_under_test_dir(file_name: str) -> bool:
    """
    Return whether a file is under a test directory (which is called "test").
    """
    return _is_under_dir(file_name, "test")


def is_test_input_output_file(file_name: str) -> bool:
    """
    Return whether a file is used as input or output in a unit test.
    """
    ret = is_under_test_dir(file_name)
    ret &= file_name.endswith(".txt")
    return ret


def is_test_code(file_name):
    ret = is_under_test_dir(file_name)
    ret &= os.path.basename(file_name).startswith("test_")
    ret &= file_name.endswith(".py")
    return ret


def is_py_file(file_name: str) -> bool:
    """
    Return whether a file is a python file.
    """
    return file_name.endswith(".py")


def is_ipynb_file(file_name: str) -> bool:
    """
    Return whether a file is a jupyter notebook file.
    """
    return file_name.endswith(".ipynb")


def from_python_to_ipynb_file(file_name: str) -> str:
    dbg.dassert(is_py_file(file_name))
    ret = file_name.replace(".py", ".ipynb")
    return ret


def from_ipynb_to_python_file(file_name):
    dbg.dassert(is_ipynb_file(file_name))
    ret = file_name.replace(".ipynb", ".py")
    return ret


def is_paired_jupytext_file(file_name: str) -> bool:
    """
    Return whether a file is a paired jupytext file.
    """
    is_paired = (
        is_py_file(file_name)
        and os.path.exists(from_python_to_ipynb_file(file_name))
        or (
            is_ipynb_file(file_name)
            and os.path.exists(from_ipynb_to_python_file(file_name))
        )
    )
    return is_paired


class _ProcessJupytext(_Action):
    def __init__(self, jupytext_action):
        executable = "process_jupytext.py"
        super().__init__(executable)
        self._jupytext_action = jupytext_action

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        _ = pedantic
        output: List[str] = []
        # TODO(gp): Use the usual idiom of these functions.
        if is_py_file(file_name) and is_paired_jupytext_file(file_name):
            cmd_opts = "-f %s --action %s" % (file_name, self._jupytext_action)
            cmd = self._executable + " " + cmd_opts
            output = _tee(cmd, self._executable, abort_on_error=True)
        else:
            _LOG.debug("Skipping file_name='%s'", file_name)
        return output


class _SyncJupytext(_ProcessJupytext):
    def __init__(self):
        super().__init__("sync")


class _TestJupytext(_ProcessJupytext):
    def __init__(self):
        super().__init__("test")


# ##############################################################################


class _LintMarkdown(_Action):
    def __init__(self):
        executable = "prettier"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        return _check_exec(self._executable)

    def _execute(self, file_name: str, pedantic: bool) -> List[str]:
        # Applicable only to txt and md files.
        ext = os.path.splitext(file_name)[1]
        output: List[str] = []
        if ext not in (".txt", ".md"):
            _LOG.debug("Skipping file_name='%s' because ext='%s'", file_name, ext)
            return output
        #
        # Pre-process text.
        #
        txt = io_.from_file(file_name, split=True)
        txt_new: List[str] = []
        for line in txt:
            line = re.sub(r"^\* ", "- STAR", line)
            txt_new.append(line)
        # Write.
        txt_new_as_str = "\n".join(txt_new)
        io_.to_file(file_name, txt_new_as_str)
        #
        # Lint.
        #
        cmd_opts: List[str] = []
        cmd_opts.append("--parser markdown")
        cmd_opts.append("--prose-wrap always")
        cmd_opts.append("--write")
        cmd_opts.append("--tab-width 4")
        cmd_opts_as_str = " ".join(cmd_opts)
        cmd_as_str = " ".join([self._executable, cmd_opts_as_str, file_name])
        output_tmp = _tee(cmd_as_str, self._executable, abort_on_error=True)
        output.extend(output_tmp)
        #
        # Post-process text.
        #
        txt = io_.from_file(file_name, split=True)
        txt_new: List[str] = []  # type: ignore
        for i, line in enumerate(txt):
            # Check whether there is TOC otherwise add it.
            if i == 0 and line != "<!--ts-->":
                output.append(
                    "No tags for table of content in md file: adding it"
                )
                line = "<!--ts-->\n<!--te-->"
            line = re.sub(r"^\-   STAR", "*   ", line)
            # Remove some artifacts when copying from gdoc.
            line = re.sub(r"’", "'", line)
            line = re.sub(r"“", '"', line)
            line = re.sub(r"”", '"', line)
            line = re.sub(r"…", "...", line)
            # -   You say you'll do something
            # line = re.sub("^(\s*)-   ", r"\1- ", line)
            # line = re.sub("^(\s*)\*   ", r"\1* ", line)
            txt_new.append(line)
        # Write.
        txt_new_as_str = "\n".join(txt_new)  # type: ignore
        io_.to_file(file_name, txt_new_as_str)
        #
        # Refresh table of content.
        #
        amp_path = git.get_amp_abs_path()
        cmd: List[str] = []  # type: ignore
        cmd.append(os.path.join(amp_path, "scripts/gh-md-toc"))
        cmd.append("--insert %s" % file_name)
        cmd_as_str = " ".join(cmd)
        _system(cmd_as_str, abort_on_error=False)
        return output


# #############################################################################
# Actions.
# #############################################################################

# We use the command line instead of API because:
# - some tools don't have a public API
# - this make easier to reproduce / test commands using the command lines and
#   then incorporate in the code
# - it allows to have clear control over options


def _get_action_class(action: str) -> _Action:
    """
    Return the function corresponding to the passed string.
    """
    res = None
    for action_meta in _VALID_ACTIONS_META:
        name, rw, comment, class_ = action_meta
        _ = rw, comment
        if name == action:
            dbg.dassert_is(res, None)
            res = class_
    # Make mypy happy.
    if res is None:
        dbg.dassert_is_not(res, None)
    else:
        obj = res()
    return obj


def _remove_not_possible_actions(actions: List[str]) -> List[str]:
    """
    Check whether each action in "actions" can be executed and return a list of
    the actions that can be executed.

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


def _actions_to_string(actions: List[str]) -> str:
    space = max([len(a) for a in _ALL_ACTIONS]) + 2
    format_ = "%" + str(space) + "s: %s"
    actions_as_str = [
        format_ % (a, "Yes" if a in actions else "-") for a in _ALL_ACTIONS
    ]
    return "\n".join(actions_as_str)


def _select_actions(args: argparse.Namespace) -> List[str]:
    # Select actions.
    actions = args.action
    if isinstance(actions, str) and " " in actions:
        actions = actions.split(" ")
    if not actions or args.all:
        actions = _ALL_ACTIONS[:]
    # Validate actions.
    for action in set(actions):
        if action not in _ALL_ACTIONS:
            raise ValueError("Invalid action '%s'" % action)
    # Reorder actions according to _ALL_ACTIONS.
    actions = [action for action in _ALL_ACTIONS if action in actions]
    # Find the tools that are available.
    actions = _remove_not_possible_actions(actions)
    #
    actions_as_str = _actions_to_string(actions)
    _LOG.info("# Action selected:\n%s", pri.space(actions_as_str))
    return actions


def _test_actions():
    _LOG.info("Testing actions")
    # Check all the actions.
    num_not_poss = 0
    possible_actions: List[str] = []
    for action in _ALL_ACTIONS:
        class_ = _get_action_class(action)
        is_possible = class_.check_if_possible()
        _LOG.debug("%s -> %s", action, is_possible)
        if is_possible:
            possible_actions.append(action)
        else:
            num_not_poss += 1
    # Report results.
    actions_as_str = _actions_to_string(possible_actions)
    _LOG.info("Possible actions:\n%s", pri.space(actions_as_str))
    if num_not_poss > 0:
        _LOG.warning("There are %s actions that are not possible", num_not_poss)
    else:
        _LOG.info("All actions are possible")


# #############################################################################


def _lint(
    file_name: str, actions: List[str], pedantic: bool, debug: bool
) -> List[str]:
    """
    Execute all the actions on a filename.

    Note that this is the unit of parallelization, i.e., we run all the
    actions on a single file to ensure that the actions are executed in the
    proper order.
    """
    output: List[str] = []
    _LOG.info("\n%s", pri.frame(file_name, char1="="))
    for action in actions:
        _LOG.debug("\n%s", pri.frame(action, char1="-"))
        print("## %-20s (%s)" % (action, file_name))
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
        _dassert_list_of_strings(
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
    # Use serial mode if there is a single file, unless the user specified
    # explicitly the numer of threads to use.
    if len(file_names) == 1 and num_threads != -1:
        num_threads = "serial"
        _LOG.warning(
            "Using num_threads='%s' since there is a single file", num_threads
        )
    if num_threads == "serial":
        output: List[str] = []
        for file_name in file_names:
            output_tmp = _lint(file_name, actions, pedantic, args.debug)
            output.extend(output_tmp)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info(
            "Using %s threads", num_threads if num_threads > 0 else "all CPUs"
        )
        from joblib import Parallel, delayed

        output_tmp = Parallel(n_jobs=num_threads, verbose=50)(
            delayed(_lint)(file_name, actions, pedantic, args.debug)
            for file_name in file_names
        )
        output = list(itertools.chain.from_iterable(output_tmp))
    output.insert(0, "cmd line='%s'" % dbg.get_command_line())
    # TODO(gp): datetime_.get_timestamp().
    # output.insert(1, "datetime='%s'" % datetime.datetime.now())
    output = _remove_empty_lines(output)
    return output


# #############################################################################
# Main.
# #############################################################################

# Actions and if they read / write files.
# The order of this list implies the order in which they are executed.
_VALID_ACTIONS_META = [
    (
        "check_file_property",
        "r",
        "Check that generic files are valid",
        _CheckFileProperty,
    ),
    (
        "basic_hygiene",
        "w",
        "Clean up (e.g., tabs, trailing spaces)",
        _BasicHygiene,
    ),
    ("compile_python", "r", "Check that python code is valid", _CompilePython),
    ("autoflake", "w", "Removes unused imports and variables", _Autoflake),
    ("isort", "w", "Sort Python import definitions alphabetically", _Isort),
    # Superseded by black.
    # ("yapf", "w", "Formatter for Python code", _Yapf),
    ("black", "w", "The uncompromising code formatter", _Black),
    ("flake8", "r", "Tool For Style Guide Enforcement", _Flake8),
    ("pydocstyle", "r", "Docstring style checker", _Pydocstyle),
    # TODO(gp): Fix this.
    # Not installable through conda.
    # ("pyment", "w", "Create, update or convert docstring", _Pyment),
    ("pylint", "w", "Check that module(s) satisfy a coding standard", _Pylint),
    ("mypy", "r", "Static code analyzer using the hint types", _Mypy),
    ("sync_jupytext", "w", "Create / sync jupytext files", _SyncJupytext),
    ("test_jupytext", "r", "Test jupytext files", _TestJupytext),
    # Superseded by "sync_jupytext".
    # ("ipynb_format", "w", "Format jupyter code using yapf", _IpynbFormat),
    ("lint_markdown", "w", "Lint txt/md markdown files", _LintMarkdown),
]

_ALL_ACTIONS = list(zip(*_VALID_ACTIONS_META))[0]


def _main(args: argparse.Namespace) -> int:
    dbg.init_logger(args.log_level)
    #
    if args.test_actions:
        _LOG.warning("Testing actions...")
        _test_actions()
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    output: List[str] = []
    # Get all the files to process.
    all_file_names = _get_files(args)
    _LOG.info("Found %s files to process", len(all_file_names))
    # Select files.
    file_names = _get_files_to_lint(args, all_file_names)
    _LOG.info(
        "# Found %d files to lint:\n%s",
        len(file_names),
        pri.space("\n".join(file_names)),
    )
    if args.collect_only:
        _LOG.warning("Exiting as requested")
        sys.exit(0)
    # Select actions.
    actions = _select_actions(args)
    _LOG.debug("actions=%s", actions)
    # Create tmp dir.
    io_.create_dir(_TMP_DIR, incremental=False)
    _LOG.info("tmp_dir='%s'", _TMP_DIR)
    # Check the files.
    action = "check_file_property"
    if action in actions:
        for file_name in all_file_names:
            pedantic = args.pedantic
            class_ = _get_action_class(action)
            output_tmp = class_.execute(file_name, pedantic)
            _dassert_list_of_strings(output_tmp)
            output.extend(output_tmp)
    actions = [a for a in actions if a != action]
    _LOG.debug("actions=%s", actions)
    # Run linter.
    output_tmp = _run_linter(actions, args, file_names)
    _dassert_list_of_strings(output_tmp)
    output.extend(output_tmp)
    # Sort the errors.
    output = sorted(output)
    # Print linter output.
    print(pri.frame(args.linter_log, char1="/").rstrip("\n"))
    print("\n".join(output) + "\n")
    print(pri.line(char="/").rstrip("\n"))
    # Write the file.
    output_as_str = "\n".join(output)
    io_.to_file(args.linter_log, output_as_str)
    # Count number of lints.
    num_lints = 0
    for line in output:
        # dev_scripts/linter.py:493: ... [pydocstyle]
        if re.match(r"\S+:\d+.*\[\S+\]", line):
            num_lints += 1
    _LOG.info("num_lints=%d", num_lints)
    if num_lints != 0:
        _LOG.info(
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


def _parser() -> argparse.ArgumentParser:
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
        help="Select all files modified in the current git client",
    )
    parser.add_argument(
        "-p",
        "--previous_git_commit_files",
        nargs="?",
        type=int,
        const=1,
        default=None,
        help="Select all files modified in previous 'n' user git commit",
    )
    parser.add_argument(
        "-d",
        "--dir_name",
        action="store",
        help="Select all files in a dir. 'GIT_ROOT' to select git root",
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
    parser.add_argument("--action", action="append", help="Run a specific check")
    parser.add_argument(
        "--all", action="store_true", help="Run all recommended phases"
    )
    parser.add_argument(
        "--pedantic", action="store_true", help="Run some purely cosmetic lints"
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
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    parser_ = _parser()
    args_ = parser_.parse_args()
    rc_ = _main(args_)
    sys.exit(rc_)
