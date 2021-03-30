import logging
import os

import vendors_amp.ib.data.extract.gateway.metadata as viemet

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")


import helpers.dbg as dbg
import helpers.unit_test as hut
import vendors_amp.ib.data.extract.gateway.utils as vieuti

_LOG = logging.getLogger(__name__)


class Test_ib_metadata1(hut.TestCase):
    @classmethod
    def setUpClass(cls):
        dbg.shutup_chatty_modules()
        cls.ib = vieuti.ib_connect(0, is_notebook=False)

    @classmethod
    def tearDownClass(cls):
        cls.ib.disconnect()

    def test1(self) -> None:
        """
        Create some metadata for NG.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = viemet.IbMetadata(file_name)
        #
        symbol = "NG"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract])
        #
        df = ibmeta.load()
        self.check_string(df.to_csv())

    def test2(self) -> None:
        """
        Create some metadata and then update more.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = viemet.IbMetadata(file_name)
        #
        symbol = "NG"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract])
        #
        symbol = "CL"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract], append=True)
        #
        df = ibmeta.load()
        self.check_string(df.to_csv())

    def test3(self) -> None:
        """
        Test that append=False cleans up the file.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = viemet.IbMetadata(file_name)
        #
        symbol = "NG"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract])
        #
        symbol = "CL"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract], append=False)
        #
        df = ibmeta.load()
        self.check_string(df.to_csv())
