import logging
import os

import pandas as pd

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")

import helpers.dbg as dbg
import helpers.unit_test as hut
import im.ib.data.extract.gateway.metadata as iidegm
import im.ib.data.extract.gateway.utils as iidegu

_LOG = logging.getLogger(__name__)


@pytest.mark.skip(msg="See alphamatic/dev_tools#282")
class Test_ib_metadata1(hut.TestCase):
    @classmethod
    def setUpClass(cls):
        dbg.shutup_chatty_modules()
        cls.ib = iidegu.ib_connect(0, is_notebook=False)

    @classmethod
    def tearDownClass(cls):
        cls.ib.disconnect()

    def test1(self) -> None:
        """
        Create some metadata for NG.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = iidegm.IbMetadata(file_name)
        #
        symbol = "NG"
        contract = ib_insync.Future(symbol, includeExpired=True)
        ibmeta.update(self.ib, [contract])
        #
        df = ibmeta.load()
        #
        self._check_metadata_df(df)

    def test2(self) -> None:
        """
        Create some metadata and then update more.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = iidegm.IbMetadata(file_name)
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
        self._check_metadata_df(df)

    def test3(self) -> None:
        """
        Test that append=False cleans up the file.
        """
        file_name = os.path.join(self.get_scratch_space(), "metadata.csv")
        ibmeta = iidegm.IbMetadata(file_name)
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
        self._check_metadata_df(df)

    def _check_metadata_df(self, df: pd.DataFrame) -> None:
        """
        Filter contracts by expiration date to not update outcomes regularly.
        """
        filtered_df = df[
            (df["lastTradeDateOrContractMonth"] >= 20210101)
            & (df["lastTradeDateOrContractMonth"] < 20220101)
        ]
        filtered_df = filtered_df.reset_index(drop=True)
        self.check_string(filtered_df.to_csv())
