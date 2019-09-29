import glob
import logging
import os

import pytest

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# TODO(gp): Generalize to all users, or at least Jenkins.
@pytest.mark.skipif('si.get_user_name() != "saggese"')
class Test_pandoc1(ut.TestCase):
    def _helper(self, in_file):
        git_dir = git.get_client_root(super_module=False)
        exec_path = os.path.join(git_dir, "docs/scripts/pandoc.py")
        dbg.dassert_exists(exec_path)
        #
        tmp_dir = self.get_scratch_space()
        out_file = os.path.join(tmp_dir, "output.pdf")
        cmd = []
        cmd.append(exec_path)
        cmd.append("-a pdf")
        cmd.append("--tmp_dir %s" % tmp_dir)
        cmd.append("--input %s" % in_file)
        cmd.append("--output %s" % out_file)
        cmd.append("--no_open_pdf")
        cmd.append("--no_gdrive")
        cmd.append("--no_cleanup")
        cmd = " ".join(cmd)
        si.system(cmd)
        # Check.
        out_file = os.path.join(tmp_dir, "tmp.pandoc.tex")
        act = io_.from_file(out_file, split=False)
        return act

    def test1(self):
        """
        Convert to pdf one txt file and check that the .tex file is as expected.
        """
        file_name = "code_style.txt.test"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        act = self._helper(file_name)
        self.check_string(act)

    def test_all_notes(self):
        """
        Convert to pdf all the notes in docs/notes.
        """
        git_dir = git.get_client_root(super_module=False)
        dir_name = os.path.join(git_dir, "docs/notes/*.txt")
        file_names = glob.glob(dir_name)
        for f in file_names:
            _LOG.debug(prnt.frame("file_name=%s" % f))
            self._helper(f)


class Test_preprocess1(ut.TestCase):
    def _helper(self):
        """
        Check that the output of remove_md_empty_lines.py is the expected one.
        """
        git_dir = git.get_client_root(super_module=False)
        exec_path = os.path.join(git_dir, "docs/scripts/remove_md_empty_lines.py")
        dbg.dassert_exists(exec_path)
        #
        in_file = os.path.join(self.get_input_dir(), "input1.txt")
        out_file = os.path.join(self.get_scratch_space() + "output.txt")
        cmd = []
        cmd.append(exec_path)
        cmd.append("--input %s" % in_file)
        cmd.append("--output %s" % out_file)
        cmd = " ".join(cmd)
        si.system(cmd)
        # Check.
        act = io_.from_file(out_file, split=False)
        self.check_string(act)

    def test1(self):
        self._helper()

    def test2(self):
        self._helper()
