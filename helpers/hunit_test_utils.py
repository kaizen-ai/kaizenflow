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


class UnitTestOutcomeRenamer():
    def __init__(self, old_class: str, new_class: str, old_method: str, new_method: str) -> None:
        """
        Initialize TestOutcomeRenamer class.

        :param old_class: the name of the old class
        :param new_class: the name of the new class
        :param old_method: the name of the old method
        :param new_method: the name of the new method
        """
        self.old_class = old_class
        self.new_class = new_class
        self.old_method = old_method
        self.new_method = new_method


    def run(
        self,
        path: str,
        ) -> None:
        """
        Rename the directory that contains test outcomes.

        :param path: the path to the test directory, e.g. `cmamp1/helpers/test/`
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
        for outcome_dir in outcomes:
            renamed = self.process_directory(outcome_dir, outcomes_path)
        if not renamed:
            _LOG.info(
                "No outcomes for `%s` were found in `%s`.",
                self.old_class,
                outcomes_path,
            )


    def rename_outcomes(self, outcome_path_old: str, outcome_path_new: str) -> None:
        """
        Rename the outcomes directory and add it to git.

        :param outcome_path_old:
        :param outcome_path_new:
        :return:
        """
        cmd = f"mv {outcome_path_old} {outcome_path_new}"
        # Rename the directory.
        rc = hsystem.system(cmd, abort_on_error=True, suppress_output=False)
        _LOG.info(
            "Renaming `%s` directory to `%s`. Output log: %s",
            outcome_path_old,
            outcome_path_new,
            rc,
        )
        # Add to git new outcome directory and remove the old one.
        # The sequence of commands is used
        # git mv does not work properly while unit testing
        cmd = f"git add {outcome_path_new} && git rm -r {outcome_path_old}"
        hsystem.system(cmd, abort_on_error=True, suppress_output=False)
    

    def process_directory(self, outcome_dir: str, outcomes_path: str) -> bool:
        """
        Process the directory containing target test outcomes.

        The stages of processing are:
         - generate the new name of the directory
         - rename and add it to git

        :param outcome_dir: the name of the directory containing the outcomes
        :param outcomes_path: the path to the outcomes directory
        """
        # Contruct the path to outcomes directory.
        outcome_path_old = os.path.join(outcomes_path, outcome_dir)
        # Construct target old and new target dir names, e.g. 
        # `TestOldName.` and `TestNewName.` if class should be renamed or
        # `TestOldName.test_old` and `TestOldName.test_new` if method should be renamed.
        old_target = ".".join([self.old_class, self.old_method])
        new_target = ".".join([self.new_class, self.new_method])
        if self.old_method == "" and outcome_dir.startswith(old_target):
            # Check if the class should be renamed, e.g. `outcome_dir` is `TestOld.test1` and `old_target` is `TestOld.`.
            # Split old directory name - the part before "." is the class name.
            class_method = outcome_dir.split(".")
            # Replace old class name with the new one, `["TestOld", "test1"]` -> `["TestNew", "test1"]`.
            class_method[0] = self.new_class
            # Construct the new outcome directory name -> `TestNew.test1`.
            outcome_name_new = ".".join(class_method)
            outcome_path_new = os.path.join(outcomes_path, outcome_name_new)
        elif self.old_method != "" and (outcome_dir == old_target):
            # Check if the method should be renamed, e.g. `outcome_dir` is `TestOld.test1` and `old_target` is `TestOld.test1_new`.
            outcome_path_new = os.path.join(outcomes_path, new_target)
        else:
            return False
        # Rename the directory and add it to git.
        self.rename_outcomes(outcome_path_old, outcome_path_new)
        return True


class UnitTestRenamer():
    def __init__(self, old_test_name: str, new_test_name: str, root_dir: str) -> None:
        """
        Initialize UnitTestRenamer class.

        :param old_test_name: the old name of the test
        :param new_test_name: the new name of the test
        :param root_dir: the dir to start the search from
        """
        # Check if the names of the test are valid.
        self._check_names(old_test_name, new_test_name)
        # Get the directories containing tests.
        self.test_dirs = self._get_test_directories(root_dir)
        # Construct the renaming config.
        self.cfg = self._process_parameters(old_test_name, new_test_name)
        # Initialize tool for test outcomes renaming.
        self.outcomes_renamer = UnitTestOutcomeRenamer(self.cfg["old_class"], self.cfg["new_class"], self.cfg["old_method"], self.cfg["new_method"])


    def run(self):
        """
        Run the renamer tool.
        """
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
                )


    def _rename_in_file(
        self,
        test_dir: str,
        file_path: str,
    ) -> None:
        """
        Process the file:

        - check if the content of the file contains target class
        - change the class name / change the method name
        - rename the outcomes if they exist

        :param test_dir: the path to the test directory containing the file
        :param file_path: the path to the file
        """
        content = hio.from_file(file_path)
        if not re.search(f"class {self.cfg['old_class_name']}\(", content):
            # Return if target test class does not appear in file content.
            return
        if self.cfg["old_method_name"] == "":
            # Rename the class.
            content = self._rename_class(content)
            _LOG.info(
                "%s: class `%s` was renamed to `%s`.",
                file_path,
                self.cfg["old_class"],
                self.cfg["new_class"],
            )
        else:
            # Rename the method of the class.
            content = self._rename_method(
                file_path, content
            )
            _LOG.info(
                "%s: method `%s` of `%s` class was renamed to `%s`.",
                file_path, self.cfg["old_method"], self.cfg["old_class"], self.cfg["new_method"]
            )
        # Rename the directories that contain target test outcomes.
        self._rename_outcomes(
            test_dir,
        )
        # Write processed content back to file.
        hio.to_file(file_path, content)


    def _rename_class(
        self, 
        content: str,
    ) -> str:
        """
        Rename a class in a Python file.

        :param content: the content of the file
        :return: the content of the file with the class name replaced
        """
        # Rename the class.
        content = re.sub(
            f"class {self.cfg['old_class_name']}\(", f"class {self.cfg['new_class_name']}(", content
        )
        return content

    def _rename_method(
        self,
        content: str,
    ) -> str:
        """
        Rename the method of the class.

        :param content: the content of the file
        :return: content of the file with the method renamed
        """
        lines = content.split("\n")
        # Flag that informs if the class border was found.
        class_found = False
        method_replaced = False
        class_pattern = f"class {self.cfg['old_class']}\("
        method_pattern = f"def {self.cfg['old_method']}\("
        for ind, line in enumerate(lines):
            # Iterate over the lines of the file to find the specific method of the class that should be renamed.
            if class_found:
                # Break if the next class started and the method was not found.
                if line.startswith("class"):
                    break
                if re.search(method_pattern, line):
                    # Rename the method.
                    new_line = re.sub(method_pattern, f"def {self.cfg['new_method']}(", line)
                    # Replace the line with method definition.
                    lines[ind] = new_line
                    method_replaced = True
                    break
            else:
                if re.search(class_pattern, line):
                    class_found = True
        hdbg.dassert(
            method_replaced,
            f"Invalid method: `{self.cfg['old_method']}` was not find in class `{self.cfg['old_class']}`",
        )
        new_content = "\n".join(lines)
        return new_content

    @staticmethod
    def _get_test_directories(root_dir: str) -> List[str]:
        """
        Get paths of the all directories that contain unit tests.

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
        """
        Check if the test names are valid.

        :param old_test_name: the old name of the test
        :param new_test_name: the new name of the test
        """
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
        old_test_name: str, new_test_name: str,
    ) -> Dict[str, Union[bool, str]]:
        """
        Build the processing config with the renaming parameters.

        Config includes the following fields:
          - `old_class`: old name of the class
          - `new_class`: new name of the class
          - `old_method`: new name of the method. If empty, only class should be renamed
          - `new_method`: new name of the method

        :param old_test_name: the old name of the test
        :param new_test_name: the new name of the test
        :return: config for renaming process
        """
        # Build the processing config.
        config: Dict[str, Union[bool, str]] = dict()
        # Split by "." to separate class name and method name.
        splitted_old_name = old_test_name.split(".")
        splitted_new_name = new_test_name.split(".")
        # Check the consistency of the names - they should have the same length.
        hdbg.dassert_eq(len(splitted_old_name)==len(splitted_new_name), "The test names are not consistent.")
        # Check the format of test names.
        if len(splitted_old_name) == 1:
            # Class name splitted by `.` is one element array, e.g. `["TestClassName"]`.
            old_class_name, old_method_name = splitted_old_name[0], ""
            new_class_name, new_method_name = splitted_new_name[0], ""
            _LOG.debug(
                f"Trying to change the name of `{old_test_name}` unit test class to `{new_test_name}`."
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
        config["old_class"] = old_class_name
        config["old_method"] = old_method_name
        config["new_class"] = new_class_name
        config["new_method"] = new_method_name
        return config