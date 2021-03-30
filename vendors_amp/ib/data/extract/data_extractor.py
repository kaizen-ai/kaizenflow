"""
Extract data from IB Gateway and put it to S3.
"""
import logging
import os
from typing import List, Optional, Tuple

import ib_insync
import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors_amp.common.data.extract.data_extractor as vcdeda
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.ib.data.extract.gateway.download_data_ib_loop as videgd
import vendors_amp.ib.data.extract.gateway.utils as videgu
import vendors_amp.ib.data.load.file_path_generator as vidlfi

_LOG = logging.getLogger(__name__)


class IbDataExtractor(vcdeda.AbstractDataExtractor):
    """
    Load data from IB and save it to S3.
    """

    _MAX_IB_CONNECTION_ATTEMPTS = 1000
    _MAX_IB_DATA_LOAD_ATTEMPTS = 3

    def __init__(self, ib_connect_client_id: Optional[int] = None):
        if ib_connect_client_id is not None:
            self._ib_connect_client_id = ib_connect_client_id
        else:
            self._ib_connect_client_id = videgu.get_free_client_id(
                self._MAX_IB_CONNECTION_ATTEMPTS
            )

    def extract_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        incremental: Optional[bool] = None,
        dst_dir: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract the data, save it and return all data for symbol.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param start_ts: start time of data to extract,
            by default - the oldest available
        :param end_ts: end time of data to extract,
            by default - now
        :param incremental: if True - save only new data,
            if False - remove old firstly,
            True by default
        :param dst_dir: place to keep results of each IB request
        :return: a dataframe with the data
        """
        part_files_dir = (
            self.get_default_part_files_dir(
                symbol=symbol,
                frequency=frequency,
                asset_class=asset_class,
                contract_type=contract_type,
            )
            if dst_dir is None
            else dst_dir
        )
        self.extract_data_parts_with_retry(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            start_ts=start_ts,
            end_ts=end_ts,
            incremental=incremental,
            part_files_dir=part_files_dir,
        )
        # Union all data from files and save to archive.
        saved_data = self.update_archive(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            part_files_dir=part_files_dir,
        )
        return saved_data

    def extract_data_parts_with_retry(
        self,
        part_files_dir: str,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        incremental: Optional[bool] = None,
    ) -> None:
        """
        Extract the data by chunks and save them.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param start_ts: start time of data to extract,
            by default - the oldest available
        :param end_ts: end time of data to extract,
            by default - now
        :param incremental: if True - save only new data,
            if False - remove old firstly,
            True by default
        :param part_files_dir: place to keep results of each IB request
        """
        # Connect to IB.
        ib_connection = videgu.ib_connect(
            self._ib_connect_client_id, is_notebook=False
        )
        # Save extracted data in parts.
        left_intervals = [(start_ts, end_ts)]
        num_attempts_done = 0
        while (
            left_intervals and num_attempts_done < self._MAX_IB_DATA_LOAD_ATTEMPTS
        ):
            left_intervals_after_try = []
            for interval in left_intervals:
                # Try to extract the data. Save unsuccessful intervals.
                failed_intervals = self._extract_data_parts(
                    ib=ib_connection,
                    exchange=exchange,
                    symbol=symbol,
                    asset_class=asset_class,
                    frequency=frequency,
                    contract_type=contract_type,
                    start_ts=interval[0],
                    end_ts=interval[1],
                    incremental=incremental or bool(num_attempts_done),
                    part_files_dir=part_files_dir,
                )
                left_intervals_after_try.extend(failed_intervals)
            num_attempts_done += 1
            left_intervals = left_intervals_after_try.copy()
        # Disconnect from IB.
        ib_connection.disconnect()

    @classmethod
    def update_archive(
        cls,
        part_files_dir: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
    ) -> pd.DataFrame:
        """
        Read data from parts, save it to archive.

        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param part_files_dir: place to keep results of each IB request
        :return: a dataframe with the data
        """
        # Find main archive file location.
        arch_file = vidlfi.IbFilePathGenerator().generate_file_path(
            symbol=symbol,
            frequency=frequency,
            asset_class=asset_class,
            contract_type=contract_type,
            ext=vcdtyp.Extension.CSV,
        )
        _, arch_name = os.path.split(arch_file)
        # Find files with partial data locations.
        part_file_names = (
            hs3.ls("%s/" % part_files_dir)
            if part_files_dir.startswith("s3://")
            else os.listdir(part_files_dir)
        )
        part_files = [
            os.path.join(part_files_dir, file_name)
            for file_name in part_file_names
        ]
        _LOG.info("Union files in `%s` to `%s`", part_files_dir, arch_file)
        # Read data.
        data: pd.DataFrame = pd.concat(
            [
                videgd.load_historical_data(part_file)
                for part_file in part_files
                if part_file != arch_name
            ]
        )
        # Sort index.
        data = data.sort_index(ascending=True)
        dbg.dassert_monotonic_index(data)
        # Save data to archive.
        data.to_csv(arch_file, compression="gzip")
        _LOG.info("Finished, data in `%s`", arch_file)
        return data

    @staticmethod
    def get_default_part_files_dir(
        symbol: str,
        frequency: vcdtyp.Frequency,
        asset_class: vcdtyp.AssetClass,
        contract_type: vcdtyp.ContractType,
    ) -> str:
        """
        Return a `symbol` directory on S3 near the main archive file.
        """
        arch_file = vidlfi.IbFilePathGenerator().generate_file_path(
            symbol=symbol,
            frequency=frequency,
            asset_class=asset_class,
            contract_type=contract_type,
            ext=vcdtyp.Extension.CSV,
        )
        arch_path, _ = os.path.split(arch_file)
        return os.path.join(arch_path, symbol)

    def _extract_data_parts(
        self,
        ib: ib_insync.ib.IB,
        part_files_dir: str,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        incremental: Optional[bool] = None,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Make a several requests to IB, each response is saved to a separate
        file.

        E.g. list of resultes files:
        - s3://external-p1/ib/futures/daily/ESH1/ESH1.20200101.20210101.csv
        - s3://external-p1/ib/futures/daily/ESH1/ESH1.20190101.20200101.csv
        - ...

        :param ib: IB connection
        :param part_files_dir: place to keep results of each IB request
        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param start_ts: start time of data to extract,
            by default - the oldest available
        :param end_ts: end time of data to extract,
            by default - now
        :param incremental: if True - save only new data,
            if False - remove old firstly,
            True by default
        :return: a list of failed intervals
        """
        # Get tasks.
        tasks = videgu.get_tasks(
            ib=ib,
            target=self._get_ib_target(asset_class, contract_type),
            frequency=self._get_ib_frequency(frequency),
            symbols=[symbol],
            start_ts=start_ts,
            end_ts=end_ts,
            use_rth=False,
            exchange=exchange,
        )
        # Do tasks.
        file_name = vidlfi.IbFilePathGenerator().generate_file_path(
            symbol=symbol,
            frequency=frequency,
            asset_class=asset_class,
            contract_type=contract_type,
            ext=vcdtyp.Extension.CSV,
        )
        failed_tasks_intervals = []
        for (
            contract,
            start_ts_task,
            end_ts_task,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        ) in tasks:
            saved_intervals = videgd.save_historical_data_by_intervals_IB_loop(
                ib=ib,
                contract=contract,
                start_ts=start_ts_task,
                end_ts=end_ts_task,
                duration_str=duration_str,
                bar_size_setting=bar_size_setting,
                what_to_show=what_to_show,
                use_rth=use_rth,
                file_name=file_name,
                part_files_dir=part_files_dir,
                incremental=incremental,
                num_retry=self._MAX_IB_DATA_LOAD_ATTEMPTS,
            )
            # Find intervals with no data.
            for interval in saved_intervals:
                file_name_for_part = videgd.historical_data_to_filename(
                    contract=contract,
                    start_ts=interval[0],
                    end_ts=interval[1],
                    duration_str=duration_str,
                    bar_size_setting=bar_size_setting,
                    what_to_show=what_to_show,
                    use_rth=use_rth,
                    dst_dir=part_files_dir,
                )
                df_part = videgd.load_historical_data(file_name_for_part)
                if df_part.empty:
                    failed_tasks_intervals.append(interval)
        # Return failed intervals.
        return failed_tasks_intervals

    @staticmethod
    def _get_ib_target(
        asset_class: vcdtyp.AssetClass,
        contract_type: Optional[vcdtyp.ContractType],
    ) -> str:
        """
        Transform asset to a format known by IB gateway code.
        """
        target: str
        if (
            asset_class == vcdtyp.AssetClass.Futures
            and contract_type == vcdtyp.ContractType.Continuous
        ):
            target = "continuous_futures"
        elif (
            asset_class == vcdtyp.AssetClass.Futures
            and contract_type == vcdtyp.ContractType.Expiry
        ):
            target = "futures"
        elif asset_class == vcdtyp.AssetClass.Stocks:
            target = "stocks"
        elif asset_class == vcdtyp.AssetClass.Forex:
            target = "forex"
        else:
            raise ValueError(
                "Couldn't find corresponding IB target for asset class %s and contract type %s"
                % (asset_class, contract_type)
            )
        return target

    @staticmethod
    def _get_ib_frequency(frequency: vcdtyp.Frequency) -> str:
        """
        Transform frequency to a format known by IB gateway code.
        """
        ib_frequency: str
        if frequency == vcdtyp.Frequency.Daily:
            ib_frequency = "day"
        elif frequency == vcdtyp.Frequency.Hourly:
            ib_frequency = "hour"
        elif frequency == vcdtyp.Frequency.Minutely:
            ib_frequency = "intraday"
        else:
            raise ValueError(
                "Couldn't find corresponding IB frequency for %s" % frequency
            )
        return ib_frequency
