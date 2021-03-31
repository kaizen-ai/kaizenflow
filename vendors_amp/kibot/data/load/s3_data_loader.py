import functools
import logging
from typing import Optional

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors_amp.common.data.load.s3_data_loader as vcdls3
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.kibot.data.load.file_path_generator as vkdlfi

_LOG = logging.getLogger(__name__)


# TODO(gp): -> KibotS3DataLoader
class S3KibotDataLoader(vcdls3.AbstractS3DataLoader):
    # TODO(plyq): Uncomment once #1047 will be resolved.
    # @hcache.cache
    # Use lru_cache for now.
    @functools.lru_cache(maxsize=64)
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read kibot data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        return self._read_data(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
            nrows=nrows,
            normalize=normalize,
        )

    @classmethod
    def normalize(
        cls, df: pd.DataFrame, frequency: vcdtyp.Frequency
    ) -> pd.DataFrame:
        """
        Apply a normalizer function based on the frequency.

        :param df: a dataframe that should be normalized
        :param frequency: frequency of the data
        :return: a normalized dataframe
        :raises AssertionError: if a frequency is not supported in 'Mapping'.
        """
        MAPPING = {
            vcdtyp.Frequency.Daily: cls._normalize_daily,
            vcdtyp.Frequency.Minutely: cls._normalize_1_min,
        }
        if frequency not in MAPPING:
            dbg.dfatal(
                "Support for frequency '%s' not implemented yet", frequency
            )
        normalizer = MAPPING[frequency]
        return normalizer(df)

    @classmethod
    def _read_data(
        cls,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        file_path = vkdlfi.KibotFilePathGenerator().generate_file_path(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
            ext=vcdtyp.Extension.CSV,
        )
        if hs3.is_s3_path(file_path):
            dbg.dassert_is(
                hs3.exists(file_path), True, msg=f"S3 key not found: {file_path}"
            )
        df = pd.read_csv(file_path, header=None, nrows=nrows)
        if normalize:
            df = cls.normalize(df=df, frequency=frequency)
        return df

    @staticmethod
    # TODO(gp): Call the column datetime_ET suffix.
    def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a df with 1 min Kibot data into our internal format.

        - Combine the first two columns into a datetime index
        - Add column names
        - Check for monotonic index

        :param df: kibot raw dataframe as it is in .csv.gz files
        :return: a dataframe with `datetime` index and `open`, `high`,
            `low`, `close`, `vol` columns. If the input dataframe
            has only one column, the column name will be transformed to
            string format.
        """
        # There are cases in which the dataframes consist of only one column,
        # with the first row containing a `405 Data Not Found` string, and
        # the second one containing `No data found for the specified period
        # for BTSQ14.`
        if df.shape[1] > 1:
            # According to Kibot the columns are:
            # Date,Time,Open,High,Low,Close,Volume
            # Convert date and time into a datetime.
            df[0] = pd.to_datetime(df[0] + " " + df[1], format="%m/%d/%Y %H:%M")
            df.drop(columns=[1], inplace=True)
            # Rename columns.
            df.columns = "datetime open high low close vol".split()
            df.set_index("datetime", drop=True, inplace=True)
        else:
            df.columns = df.columns.astype(str)
            _LOG.warning("The dataframe has only one column:\n%s", df)
        dbg.dassert(df.index.is_monotonic_increasing)
        dbg.dassert(df.index.is_unique)
        return df

    @staticmethod
    def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a df with daily Kibot data into our internal format.

        - Convert the first column to datetime and set is as index
        - Add column names
        - Check for monotonic index

        :param df: kibot raw dataframe as it is in .csv.gz files
        :return: a dataframe with `datetime` index and `open`, `high`,
            `low`, `close`, `vol` columns.
        """
        # Convert date and time into a datetime.
        df[0] = pd.to_datetime(df[0], format="%m/%d/%Y")
        # Rename columns.
        df.columns = "datetime open high low close vol".split()
        df.set_index("datetime", drop=True, inplace=True)
        # TODO(gp): Turn date into datetime using EOD timestamp. Check on Kibot.
        dbg.dassert(df.index.is_monotonic_increasing)
        dbg.dassert(df.index.is_unique)
        return df
