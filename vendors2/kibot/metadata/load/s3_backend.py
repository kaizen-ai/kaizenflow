import logging
import os

import pandas as pd

import helpers.dbg as dbg
import vendors2.kibot.metadata.config as mconfig

_LOG = logging.getLogger(__name__)

# TODO(Amr): Extract the 2 distinct functions here
# TODO(Amr): Map the functions onto `KibotContractType`, so you provide a type,
# and get back a dataframe with its metadata
# TODO(gp): We're fetching this from s3 & not kibot, how do we pick up the updates?
# perhaps these should be move to `transform` stage and we should output pq files?
# TODO(Amr): highlight the relation to `metadata/types`
# TODO(Amr): perhaps these functions shou

# TODO(gp): Might make sense to cache the metadata too to avoid s3 access.


class S3Backend:
    # pylint: disable=line-too-long
    @staticmethod
    def read_1min_contract_metadata() -> pd.DataFrame:
        """Read minutely contract metadata.

        Contains a mapping from all 1-min prices for each contract to a download
        path (not interesting) and a description.

        Columns are:
        - Symbol
        - Link
        - Description

        Symbol    Link                                                Description
        JY        http://api.kibot.com/?action=download&link=151...   CONTINUOUS JAPANESE YEN CONTRACT
        JYF18     http://api.kibot.com/?action=download&link=vrv...   JAPANESE YEN JANUARY 2018
        """
        # pylint: enable=line-too-long
        file_name = os.path.join(
            "s3://", mconfig.S3_PREFIX, "All_Futures_Contracts_1min.csv.gz"
        )
        _LOG.debug("file_name=%s", file_name)
        df = pd.read_csv(file_name, index_col=0)
        df = df.iloc[:, 1:]
        _LOG.debug("df=\n%s", df.head(3))
        _LOG.debug("df.shape=%s", df.shape)
        return df

    # pylint: disable=line-too-long
    @staticmethod
    def read_daily_contract_metadata() -> pd.DataFrame:
        """Read daily contract metadata.

        Same mapping as `read_1min_contract_metadata()` but for daily prices.

        Columns are:
        - Symbol
        - Link
        - Description

        Symbol    Link                                                Description
        JY        http://api.kibot.com/?action=download&link=151...   CONTINUOUS JAPANESE YEN CONTRACT
        JYF18    http://api.kibot.com/?action=download&link=vrv...    JAPANESE YEN JANUARY 2018
        """
        # pylint: enable=line-too-long
        file_name = os.path.join(
            "s3://", mconfig.S3_PREFIX, "All_Futures_Contracts_daily.csv.gz"
        )
        _LOG.debug("file_name=%s", file_name)
        df = pd.read_csv(file_name, index_col=0)
        df = df.iloc[:, 1:]
        _LOG.debug("df=\n%s", df.head(3))
        _LOG.debug("df.shape=%s", df.shape)
        return df

    # pylint: disable=line-too-long
    @staticmethod
    def read_tickbidask_contract_metadata() -> pd.DataFrame:
        """Read tick-bid-ask contract metadata.

        Mapping between symbols (both continuous and not), start date, description,
        and exchange for tickbidask.

        Columns:
        - SymbolBase
        - Symbol
        - StartDate
        - Size
        - Description
        - Exchange

        SymbolBase    Symbol    StartDate    Size(MB)    Description                           Exchange
        ES            ES        9/30/2009    50610.0     CONTINUOUS E-MINI S&P 500 CONTRACT    Chicago Mercantile Exchange Mini Sized Contrac...
        ES            ESH11     4/6/2010     891.0       E-MINI S&P 500 MARCH 2011             Chicago Mercantile Exchange Mini Sized Contrac...
        """
        # pylint: enable=line-too-long
        file_name = os.path.join(
            "s3://", mconfig.S3_PREFIX, "Futures_tickbidask.txt.gz"
        )
        _LOG.debug("file_name=%s", file_name)
        df = pd.read_csv(
            file_name, index_col=0, skiprows=5, header=None, sep="\t"
        )
        df.columns = (
            "SymbolBase Symbol StartDate Size(MB) Description Exchange".split()
        )
        df_shape = df.shape
        df.dropna(inplace=True, how="all")
        df_shape_after_dropna = df.shape
        dbg.dassert_eq(df_shape[0] - 1, df_shape_after_dropna[0])
        df.index = df.index.astype(int)
        df.index.name = None
        df["StartDate"] = pd.to_datetime(df["StartDate"])
        _LOG.debug("df=\n%s", df.head(3))
        _LOG.debug("df.shape=%s", df.shape)
        return df

    # pylint: disable=line-too-long
    @staticmethod
    def read_continuous_contract_metadata() -> pd.DataFrame:
        """Read tick-bid-ask metadata for continuous contracts.

        Returns a continuous contract subset of
        `read_tickbidask_contract_metadata()`.

        Columns:
        - SymbolBase
        - Symbol
        - StartDate
        - Size
        - Description
        - Exchange

        SymbolBase Symbol  StartDate  Size(MB)    Description                                  Exchange
        JY         JY      9/27/2009  183.0       CONTINUOUS JAPANESE YEN CONTRACT             Chicago Mercantile Exchange (CME GLOBEX)
        TY         TY      9/27/2009  180.0       CONTINUOUS 10 YR US TREASURY NOTE CONTRACT   Chicago Board Of Trade (CBOT GLOBEX)
        FV         FV      9/27/2009  171.0       CONTINUOUS 5 YR US TREASURY NOTE CONTRACT    Chicago Board Of Trade (CBOT GLOBEX)
        """
        # pylint: enable=line-too-long
        file_name = os.path.join(
            "s3://", mconfig.S3_PREFIX, "FuturesContinuous_intraday.txt.gz"
        )
        _LOG.debug("file_name=%s", file_name)
        df = pd.read_csv(
            file_name, index_col=0, skiprows=5, header=None, sep="\t"
        )
        df.columns = (
            "SymbolBase Symbol StartDate Size(MB) Description Exchange".split()
        )
        df_shape = df.shape
        df.dropna(inplace=True, how="all")
        df_shape_after_dropna = df.shape
        dbg.dassert_eq(df_shape[0] - 1, df_shape_after_dropna[0])
        df.index = df.index.astype(int)
        df.index.name = None
        df["StartDate"] = pd.to_datetime(df["StartDate"])
        _LOG.debug("df=\n%s", df.head(3))
        _LOG.debug("df.shape=%s", df.shape)
        return df

    @staticmethod
    def read_kibot_exchange_mapping() -> pd.DataFrame:
        file_name = os.path.join(
            "s3://", mconfig.S3_PREFIX, "kibot_to_exchange.csv"
        )
        kibot_to_cme_mapping = pd.read_csv(file_name, index_col="Kibot_symbol")
        return kibot_to_cme_mapping
