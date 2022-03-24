"""
Import as:

import helpers.pytest_rename_task as hpretask
"""

import glob
import logging
import os
import re
from typing import  Dict, List, Union

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


class PytestRenamer():
    def __init__(self, old_test_name: str, new_test_name: str, root_dir: str) -> None:
        self.test_dirs = self._get_test_directories(root_dir)
        self.cfg = self._process_parameters(old_test_name, new_test_name)

    def run(self):
        # Iterate over test directories.
        for path in self.test_dirs:
            _LOG.debug("Scanning `%s` directory.", path)
            search_pattern = os.path.join(path, "test_*.py")
            # Get all python test files from this directory.
            files = glob.glob(search_pattern)
            #
            for test_file in files:
                self._rename_in_file(
                    path,
                    test_file,
                    old_test_class_name,
                    new_test_class_name,
                )


    def _rename_in_file(
        self,
        test_dir: str,
        file_path: str,
        old_class_name: str,
        new_class_name: str,
    ) -> None:
        """
        Process the file:

        - check if the content of the file contains target class
        - change the class name
        - rename the outcomes if they exist

        :param test_dir: the path to the test directory containing the file
        :param file_path: the path to the file
        :param old_class_name: the old name of the class
        :param new_class_name: the new name of the class
        """
        content = hio.from_file(file_path)
        if not re.search(f"class {old_class_name}\(", content):
            # Return if target test class does not appear in file content.
            return
        # Rename the class.
        content = self._rename_class(content, old_class_name, new_class_name)
        _LOG.info(
            "%s: class `%s` was renamed to `%s`.",
            file_path,
            old_class_name,
            new_class_name,
        )
        # Rename the directories that contain target test outcomes.
        self._rename_outcomes(
            test_dir,
            old_class_name,
            new_class_name,
        )
        # Write processed content back to file.
        hio.to_file(file_path, content)

    def _rename_class(
        self, 
        content: str,
        old_class_name: str,
        new_class_name: str,
    ) -> str:
        """
        Rename the class.

        :param content: the content of the file
        :param old_class_name: the old name of the target class
        :param new_class_name: the new name of the target class
        :return: the content of the file with the class name replaced
        """
        # Rename the class.
        content = re.sub(
            f"class {old_class_name}\(", f"class {new_class_name}(", content
        )
        return content

    def _rename_method(
        self,
        content: str,
        class_name: str,
        old_method_name: str,
        new_method_name: str,
    ) -> str:
        """
        Rename the method of the class.

        :param content: the content of the file
        :param class_name: the name of the class containing the method
        :param old_method_name: the old method name
        :param new_method_name: the new method name
        :return: content of the file with the method renamed
        """
        lines = content.split("\n")
        # Flag that informs if the class border was found.
        class_found = False
        method_replaced = False
        class_pattern = f"class {class_name}\("
        method_pattern = f"def {old_method_name}\("
        for ind, line in enumerate(lines):
            # Iterate over the lines of the file to find the specific method of the class that should be renamed.
            if class_found:
                # Break if the next class started and the method was not found.
                if line.startswith("class"):
                    break
                if re.search(method_pattern, line):
                    # Rename the method.
                    new_line = re.sub(method_pattern, f"def {new_method_name}(", line)
                    # Replace the line with method definition.
                    lines[ind] = new_line
                    method_replaced = True
                    break
            else:
                if re.search(class_pattern, line):
                    class_found = True
        hdbg.dassert(
            method_replaced,
            f"Invalid method: `{old_method_name}` was not find in class `{class_name}`",
        )
        new_content = "\n".join(lines)
        return new_content

    def _rename_outcomes(
        self,
        path: str,
        old_class_name: str,
        new_class_name: str,
    ) -> None:
        """
        Rename the directory that contains test outcomes.

        :param path: the path to the test directory, e.g. `cmamp1/helpers/test/`
        :param old_class_name: the old name of the target class
        :param new_class_name: the new name of the target class
        """
        outcomes_path = os.path.join(path, "outcomes")
        dir_items = os.listdir(outcomes_path)
        # Get the list of outcomes directories.
        outcomes = [
            dir_name
            for dir_name in dir_items
            if os.path.isdir(os.path.join(outcomes_path, dir_name))
        ]
        renamed = False
        # Construct target dir name, e.g. `TestClassName.`. We need to add `.` to indicate the end of the class name.
        target_dir = old_class_name + "."
        for outcome_dir in outcomes:
            # Contruct the path to outcomes directory.
            outcome_path_old = os.path.join(outcomes_path, outcome_dir)
            # Both old and new method names should belong to one class.
            if outcome_dir.startswith(target_dir):
                # Split old directory name - the part before "." is the class name.
                class_method = outcome_dir.split(".")
                # Replace old class name with the new one.
                class_method[0] = new_class_name
                outcome_name_new = ".".join(class_method)
                outcome_path_new = os.path.join(outcomes_path, outcome_name_new)
            else:
                continue
            cmd = f"mv {outcome_path_old} {outcome_path_new}"
            # Rename the directory.
            rc = hsystem.system(cmd, abort_on_error=False, suppress_output=False)
            _LOG.info(
                "Renaming `%s` directory to `%s`. Output log: %s",
                outcome_path_old,
                outcome_path_new,
                rc,
            )
            # Add to git new outcome directory and remove the old one.
            cmd = f"git add {outcome_path_new} && git rm -r {outcome_path_old}"
            hsystem.system(cmd, abort_on_error=False, suppress_output=False)
            renamed = True
        if not renamed:
            _LOG.info(
                "No outcomes for `%s` were found in `%s`.",
                old_class_name,
                outcomes_path,
            )

    @staticmethod
    def _get_test_directories(root_dir: str) -> List[str]:
        """
        Get all paths of the directories that contain unit tests.

        :param root_dir: the dir to start the search from
        :return: paths of test directories
        """
        paths = []
        for path, _, _ in os.walk(root_dir):
            # Iterate over the paths to find the test directories.
            if path.endswith("/test"):
                paths.append(path)
        hdbg.dassert_lte(1, len(paths))
        return paths

    @staticmethod
    def _check_names(old_test_name: str, new_test_name: str) -> None:
        # Assert if the classname is invalid.
        hdbg.dassert(
            old_test_name.startswith("Test"),
            "Invalid test_class_name='%s'",
            old_test_name,
        )
        hdbg.dassert(
            new_test_name.startswith("Test"),
            "Invalid test_class_name='%s'",
            new_test_name,
        )
        hdbg.dassert_ne(old_test_name, new_test_name)
        return 

    @staticmethod
    def _process_parameters(
        old_test_class_name: str, new_test_class_name: str,
    ) -> Dict[str, Union[bool, str]]:
        """
        Build the processing config.
        """
        _check_names(old_test_class_name, new_test_class_name)
        # Build the processing config.
        config: Dict[str, Union[bool, str]] = dict()
        # Split by "." to separate class name and method name.
        splitted_old_name = old_test_class_name.split(".")
        splitted_new_name = new_test_class_name.split(".")
        # Check the consistency of the names - they should have the same length.
        hdbg.dassert_eq(len(splitted_old_name)==len(splitted_new_name), "The test names are not consistent.")
        # Check the format of test names.
        if len(splitted_old_name) == 1:
            # Class name splitted by `.` is one element array, e.g. `["TestClassName"]`.
            old_class_name, old_method_name = splitted_old_name[0], ""
            new_class_name, new_method_name = splitted_new_name[0], ""
            _LOG.debug(
                f"Trying to change the name of `{old_test_class_name}` unit test class to `{new_test_class_name}`."
            )
        elif len(splitted_old_name) == 2:
            # Method name splitted by `.` is 2 element array, e.g. `TestClassName.test2` - >`["TestClassName", "test2"]`.
            old_class_name, old_method_name = splitted_old_name
            new_class_name, new_method_name = splitted_new_name
            hdbg.dassert(
                old_class_name == new_class_name,
                "To change the name of the method, specify the methods of the same class. E.g. `--old TestCache.test1 --new TestCache.new_test1`",
            )
            _LOG.debug(
                f"Trying to change the name of `{old_method_name}` method of `{old_class_name}` class to `{new_method_name}`."
            )
        else:
            hdbg.dassert(False, "Wrong names format.")
        # Fill the processing parameters.
        config["old_class_name"] = old_class_name
        config["old_method_name"] = old_method_name
        config["new_class_name"] = new_class_name
        config["new_method_name"] = new_method_name
        return config