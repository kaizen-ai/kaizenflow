import logging

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import vendors_amp.ib_insync.utils as ibutils
import os

_LOG = logging.getLogger(__name__)

class IbMetadata:

    def __init__(self, file_name):
        self.file_name = file_name

    def load(self):
        """
        Load the data generated through update.

        The df looks like:
        conId,secType,symbol,lastTradeDateOrContractMonth,strike,right,multiplier,
            exchange,primaryExchange,currency,localSymbol,tradingClass,
            includeExpired,secIdType,secId,comboLegsDescrip,comboLegs,
            deltaNeutralContract
        81596321,FUT,NG,20190327,0.0,,10000,NYMEX,,USD,NGJ9,NG,False,,,,[],
        81596321,FUT,NG,20190327,0.0,,10000,QBALGO,,USD,NGJ9,NG,False,,,,[],
        81596324,FUT,NG,20190426,0.0,,10000,NYMEX,,USD,NGK9,NG,False,,,,[],
        """
        if os.path.exists(self.file_name):
            df = pd.read_csv(self.file_name, index_col=0)
        else:
            _LOG.debug("No file '%s'", self.file_name)
            df = pd.DataFrame()
        return df

    def update(self, ib, contracts, append=True):
        """
        Update metadata in `file_name` for the given contracts.

        :param append: if True it keeps appending
        """
        dfs = []
        if append:
            df = self.load()
            dfs.append(df)
        else:
            _LOG.warning("Resetting data in file '%s'", self.file_name)
        for contract in contracts:
            df_tmp = ibutils.get_contract_details(ib, contract)
            dfs.append(df_tmp)
        #
        df = pd.concat(dfs, axis=0)
        df.set_index("conId", drop=True, inplace=True)
        #
        hio.create_enclosing_dir(self.file_name, incremental=True)
        df.to_csv(self.file_name)