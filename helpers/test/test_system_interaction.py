import logging
import os
import re
import tempfile
from typing import List

import helpers.dbg as dbg
import helpers.system_interaction as hsyste
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_system1(hut.TestCase):
    def test1(self) -> None:
        hsyste.system("ls")

    def test2(self) -> None:
        hsyste.system("ls /dev/null", suppress_output=False)

    def test3(self) -> None:
        """
        Output to a file.
        """
        with tempfile.NamedTemporaryFile() as fp:
            temp_file_name = fp.name
            _LOG.debug("temp_file_name=%s", temp_file_name)
            hsyste.system("ls", output_file=temp_file_name)
            dbg.dassert_exists(temp_file_name)

    def test4(self) -> None:
        """
        Tee to a file.
        """
        with tempfile.NamedTemporaryFile() as fp:
            temp_file_name = fp.name
            _LOG.debug("temp_file_name=%s", temp_file_name)
            hsyste.system("ls", output_file=temp_file_name, tee=True)
            dbg.dassert_exists(temp_file_name)

    def test5(self) -> None:
        """
        Test dry_run.
        """
        temp_file_name = tempfile._get_default_tempdir()  # type: ignore
        candidate_name = tempfile._get_candidate_names()  # type: ignore
        temp_file_name += "/" + next(candidate_name)
        _LOG.debug("temp_file_name=%s", temp_file_name)
        hsyste.system("ls", output_file=temp_file_name, dry_run=True)
        dbg.dassert_not_exists(temp_file_name)

    def test6(self) -> None:
        """
        Test abort_on_error=True.
        """
        hsyste.system("ls this_file_doesnt_exist", abort_on_error=False)

    def test7(self) -> None:
        """
        Test abort_on_error=False.
        """
        with self.assertRaises(RuntimeError) as cm:
            hsyste.system("ls this_file_doesnt_exist")
        act = str(cm.exception)
        # Different systems return different rc.
        # cmd='(ls this_file_doesnt_exist) 2>&1' failed with rc='2'
        act = re.sub(r"rc='(\d+)'", "rc=''", act)
        self.check_string(act)


# #############################################################################


class Test_system2(hut.TestCase):
    def test_get_user_name(self) -> None:
        act = hsyste.get_user_name()
        _LOG.debug("act=%s", act)
        #
        exp = hsyste.system_to_string("whoami")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)
        #
        exp = hsyste.system_to_one_line("whoami")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)

    def test_get_server_name(self) -> None:
        act = hsyste.get_server_name()
        _LOG.debug("act=%s", act)
        #
        exp = hsyste.system_to_string("uname -n")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)

    def test_get_os_name(self) -> None:
        act = hsyste.get_os_name()
        _LOG.debug("act=%s", act)
        #
        exp = hsyste.system_to_string("uname -s")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)


# #############################################################################


class Test_compute_file_signature1(hut.TestCase):
    def test1(self) -> None:
        """
        Compute the signature of a file using 1 enclosing dir.
        """
        file_name = ("/app/amp/core/test/TestCheckSameConfigs." +
            "test_check_same_configs_error/output/test.txt")
        dir_depth = 1
        act = hsyste._compute_file_signature(file_name, dir_depth=dir_depth)
        exp = ["output", "test.txt"]
        self.assert_equal(str(act), str(exp))

    def test2(self) -> None:
        """
        Compute the signature of a file using 2 enclosing dirs.
        """
        file_name = ("/app/amp/core/test/TestCheckSameConfigs." +
            "test_check_same_configs_error/output/test.txt")
        dir_depth = 2
        act = hsyste._compute_file_signature(file_name, dir_depth=dir_depth)
        exp = [
            "TestCheckSameConfigs.test_check_same_configs_error",
            "output",
            "test.txt",
        ]
        self.assert_equal(str(act), str(exp))


# #############################################################################


class Test_find_file_with_dir1(hut.TestCase):
    def test1(self) -> None:
        """
        Check whether we can find this file using one enclosing dir.
        """
        # Use this file.
        file_name = "helpers/test/test_system_interaction.py"
        dir_depth = 1
        act = hsyste.find_file_with_dir(file_name, dir_depth=dir_depth)
        exp = r"""['helpers/test/test_system_interaction.py']"""
        self.assert_equal(str(act), str(exp), purify_text=True)

    def test2(self) -> None:
        """
        Check whether we can find a test golden output using different number
        of enclosing dirs.

        With only 1 enclosing dir, we can't find it.
        """
        # Use only one dir which is not enough to identify the file.
        # E.g., .../test/TestSqlWriterBackend1.test_insert_tick_data1/output/test.txt
        dir_depth = 1
        mode = "return_all_results"
        act = self._helper(dir_depth, mode)
        # For sure there are more than 100 tests.
        self.assertGreater(len(act), 100)

    def test3(self) -> None:
        """
        Like `test2`, but using 2 levels for sure we are going to identify the
        file.
        """
        dir_depth = 2
        mode = "return_all_results"
        act = self._helper(dir_depth, mode)
        _LOG.debug("Found %d matching files", len(act))
        # There should be a single match.
        exp = (
            r"""['helpers/test/Test_find_file_with_dir1.test3/output/test.txt']"""
        )
        self.assert_equal(str(act), str(exp), purify_text=True)
        self.assertEqual(len(act), 1)

    def test4(self) -> None:
        """
        Like `test2`, but using 2 levels for sure we are going to identify the
        file and asserting in case we don't find a single result.
        """
        dir_depth = 2
        mode = "assert_unless_one_result"
        act = self._helper(dir_depth, mode)
        _LOG.debug("Found %d matching files", len(act))
        # There should be a single match.
        exp = (
            r"""['helpers/test/Test_find_file_with_dir1.test4/output/test.txt']"""
        )
        self.assert_equal(str(act), str(exp), purify_text=True)
        self.assertEqual(len(act), 1)

    def test5(self) -> None:
        """
        Like `test2`, using more level than 2, again, we should have a single
        result.
        """
        dir_depth = 3
        mode = "assert_unless_one_result"
        act = self._helper(dir_depth, mode)
        _LOG.debug("Found %d matching files", len(act))
        exp = (
            r"""['helpers/test/Test_find_file_with_dir1.test5/output/test.txt']"""
        )
        self.assert_equal(str(act), str(exp), purify_text=True)
        self.assertEqual(len(act), 1)

    def _helper(self, dir_depth: int, mode: str) -> List[str]:
        # Create a fake golden outcome to be used in this test.
        act = "hello world"
        self.check_string(act)
        # E.g., helpers/test/test_system_interaction.py::Test_find_file_with_dir1::test2/test.txt
        file_name = os.path.join(self.get_output_dir(), "test.txt")
        _LOG.debug("file_name=%s", file_name)
        act = hsyste.find_file_with_dir(file_name, dir_depth=dir_depth, mode=mode)
        _LOG.debug("Found %d matching files", len(act))
        return act


# #############################################################################


class Test_Linux_commands1(hut.TestCase):
    def test_du1(self) -> None:
        hsyste.du(".")
