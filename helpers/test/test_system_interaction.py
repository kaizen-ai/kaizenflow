import logging
import re
import tempfile

import helpers.dbg as dbg
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_system1(ut.TestCase):
    def test1(self):
        si.system("ls")

    def test2(self):
        si.system("ls /dev/null", suppress_output=False)

    def test3(self):
        """
        Output to a file.
        """
        with tempfile.NamedTemporaryFile() as fp:
            temp_file_name = fp.name
            _LOG.debug("temp_file_name=%s", temp_file_name)
            si.system("ls", output_file=temp_file_name)
            dbg.dassert_exists(temp_file_name)

    def test4(self):
        """
        Tee to a file.
        """
        with tempfile.NamedTemporaryFile() as fp:
            temp_file_name = fp.name
            _LOG.debug("temp_file_name=%s", temp_file_name)
            si.system("ls", output_file=temp_file_name, tee=True)
            dbg.dassert_exists(temp_file_name)

    def test5(self):
        """
        Test dry_run.
        """
        temp_file_name = tempfile._get_default_tempdir()
        temp_file_name += "/" + next(tempfile._get_candidate_names())
        _LOG.debug("temp_file_name=%s", temp_file_name)
        si.system("ls", output_file=temp_file_name, dry_run=True)
        dbg.dassert_not_exists(temp_file_name)

    def test6(self):
        """
        Test abort_on_error=True.
        """
        si.system("ls this_file_doesnt_exist", abort_on_error=False)

    def test7(self):
        """
        Test abort_on_error=False.
        """
        with self.assertRaises(RuntimeError) as cm:
            si.system("ls this_file_doesnt_exist")
        act = str(cm.exception)
        # Different systems return different rc.
        # cmd='(ls this_file_doesnt_exist) 2>&1' failed with rc='2'
        act = re.sub("rc='(\d+)'", "rc=''", act)
        self.check_string(act)


# #############################################################################


class Test_system2(ut.TestCase):
    def test_get_user_name(self):
        act = si.get_user_name()
        _LOG.debug("act=%s", act)
        #
        exp = si.system_to_string("whoami")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)

    def test_get_server_name(self):
        act = si.get_server_name()
        _LOG.debug("act=%s", act)
        #
        exp = si.system_to_string("uname -n")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)

    def test_get_os_name(self):
        act = si.get_os_name()
        _LOG.debug("act=%s", act)
        #
        exp = si.system_to_string("uname -s")[1]
        _LOG.debug("exp=%s", exp)
        self.assertEqual(act, exp)
