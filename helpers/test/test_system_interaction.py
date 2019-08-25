import logging
import tempfile

import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_system1(ut.TestCase):

    def test1(self):
        si.system("ls")

    def test2(self):
        si.system("ls", suppress_output=False)

    def test3(self):
        with tempfile.NamedTemporaryFile() as fp:
            temp_file_name = fp.name
            si.system("ls", output_file=temp_file_name)
            si.system("ls %s" % temp_file_name, suppress_output=False)
