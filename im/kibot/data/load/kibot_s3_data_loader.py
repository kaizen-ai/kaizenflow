import logging
from typing import Optional

import pandas as pd

import core.pandas_helpers as pdhelp
import helpers.cache as hcache
import helpers.dbg as dbg
import helpers.s3 as hs3
import im.common.data.load.abstract_data_loader as icdlab
import im.common.data.types as icdtyp
import im.kibot.data.load.kibot_file_path_generator as ikdlki

_LOG = logging.getLogger(__name__)


class KibotS3DataLoader(icdlab.AbstractS3DataLoader):
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: icdtyp.AssetClass,
        frequency: icdtyp.Frequency,
        contract_type: Optional[icdtyp.ContractType] = None,
        currency: Optional[str] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Read Kibot data.
        """
        return self._read_data(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            exchange=exchange,
            currency=currency,
            unadjusted=unadjusted,
            nrows=nrows,
            normalize=normalize,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def _read_data(
        self,
        symbol: str,
        asset_class: icdtyp.AssetClass,
        frequency: icdtyp.Frequency,
        contract_type: Optional[icdtyp.ContractType] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        file_path = ikdlki.KibotFilePathGenerator().generate_file_path(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            exchange=exchange,
            currency=currency,
            unadjusted=unadjusted,
            ext=icdtyp.Extension.CSV,
        )
        data = self._read_csv(file_path, frequency, start_ts, end_ts, nrows)
        if normalize:
            data = self.normalize(df=data, frequency=frequency)
        return data

    @staticmethod
    @hcache.cache()
    def _read_csv(
        file_path: str,
        frequency: icdtyp.Frequency,
        nrows: Optional[int] = None,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Read data from S3 and cache it.
        """
        s3fs = hs3.get_s3fs("am")
        data = pdhelp.read_csv(
            file_path,
            s3fs=s3fs,
            header=None,
            nrows=nrows,
        )
        data = KibotS3DataLoader._filter_by_dates(
            data, frequency=frequency, start_ts=start_ts, end_ts=end_ts
        )
        return data

    @staticmethod
    def _filter_by_dates(
        data: pd.DataFrame,
        frequency: icdtyp.Frequency,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Filter pandas DataFrame with a date range.

        :param data: dataframe for filtering
        :param frequency: data frequency
        :param start_ts: start time of data to read. `None` means the entire data
        :param end_ts: end time of data to read. `None` means the current timestamp
        :return: filtered data
        """
        _ = frequency
        # TODO(gp): Improve this.
        if start_ts or end_ts:
            start_ts = start_ts or pd.Timestamp.min
            end_ts = end_ts or pd.Timestamp.now()
        else:
            # No need to cut the data.
            return data
        # Filter data.
        # TODO(*): implement filtering for row Kibot data.
        raise NotImplementedError
        return data

    # TODO(gp): Call the column datetime_ET suffix.
    @staticmethod
    def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a df with 1 min Kibot data into our internal format.

        - Combine the first two columns into a datetime index
        - Add column names
        - Check for monotonic index

        :param df: Kibot raw dataframe as it is in .csv.gz files
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
            #   Date, Time, Open, High, Low, Close, Volume
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

        :param df: Kibot raw dataframe as it is in .csv.gz files
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

    @staticmethod
    def _normalize_1_hour(df: pd.DataFrame) -> pd.DataFrame:
        """
        Hour data normalization. Not implemented yet.

        :param df: Pandas DataFrame for the normalization.
        :return: Normalized Pandas DataFrame
        """
        return df
