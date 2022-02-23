"""
Import as:

import im.kibot.metadata.load.kibot_metadata as imkmlkime
"""

import abc
import datetime
import logging
import os
from typing import Dict, List, Optional

import pandas as pd
import pandas.tseries.offsets as ptoffs
from tqdm.autonotebook import tqdm

import core.pandas_helpers as cpanh
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hs3 as hs3
import im.common.data.types as imcodatyp
import im.kibot.data.load.kibot_s3_data_loader as imkdlksdlo
import im.kibot.metadata.load.expiry_contract_mapper as imkmlecoma
import im.kibot.metadata.types as imkimetyp
import im_v2.kibot.metadata.client.kibot_metadata as imvkmckime

_LOG = logging.getLogger(__name__)

# Custom type.
_SymbolToContracts = Dict[str, pd.DataFrame]


# TODO(*): Move this code into a different file.
# -> FuturesContractLifetimeComputer.
class ContractLifetimeComputer(abc.ABC):
    """
    Abstract class computing the lifetime of a futures contract.
    """

    @abc.abstractmethod
    def compute_lifetime(self, contract_name: str) -> imkimetyp.ContractLifetime:
        """
        Compute the lifetime of a contract, e.g. 'CLJ17'.

        :param contract_name: the contract for which to compute the lifetime.
        :return: the computed lifetime.
        """


# TODO(*): Not sure if we should use Kibot since it's already in the package.
class KibotTradingActivityContractLifetimeComputer(ContractLifetimeComputer):
    """
    Use the price data from Kibot to compute the lifetime.
    """

    def __init__(self, end_timedelta_days: int = 0):
        """
        :param end_timedelta_days: number of days before the trading activity
            termination to mark as the end
        """
        hdbg.dassert_lte(0, end_timedelta_days)
        self.end_timedelta_days = end_timedelta_days

    def compute_lifetime(self, contract_name: str) -> imkimetyp.ContractLifetime:
        df = imkdlksdlo.KibotS3DataLoader().read_data(
            "Kibot",
            contract_name,
            imcodatyp.AssetClass.Futures,
            imcodatyp.Frequency.Daily,
            imcodatyp.ContractType.Expiry,
        )
        start_date = pd.Timestamp(df.first_valid_index())
        end_date = pd.Timestamp(df.last_valid_index())
        end_date -= ptoffs.BDay(self.end_timedelta_days)
        return imkimetyp.ContractLifetime(start_date, end_date)


class KibotHardcodedContractLifetimeComputer(ContractLifetimeComputer):
    """
    Use rules from exchange to compute the lifetime of a contract.
    """

    def __init__(self, start_timedelta_days: int, end_timedelta_days: int):
        """
        :param start_timedelta_days: number of days before the official termination
            date from the exchange that the contract starts
        :param end_timedelta_days: number of days before the official termination
            date from the exchange that the contract ends
        """
        hdbg.dassert_lte(0, start_timedelta_days)
        self.start_timedelta_days = start_timedelta_days
        hdbg.dassert_lte(0, end_timedelta_days)
        hdbg.dassert_lt(end_timedelta_days, start_timedelta_days)
        self.end_timedelta_days = end_timedelta_days
        self.ecm = imkmlecoma.ExpiryContractMapper()

    def compute_lifetime(self, contract_name: str) -> imkimetyp.ContractLifetime:
        symbol, month, year = self.ecm.parse_expiry_contract(contract_name)
        year = self.ecm.parse_year(year)
        month = self.ecm.expiry_to_month_num(month)
        if symbol == "CL":
            # https://www.cmegroup.com/trading/energy/crude-oil/light-sweet-crude_contract_specifications.html
            # "Trading terminates at the close of business on the third business day
            # prior to the 25th calendar day of the month preceding the delivery month."
            date = datetime.date(year, month, 25)
            # Closes 1 month preceding the expiry month.
            date -= pd.DateOffset(months=1)
            # Closes 3 business days before the 25th.
            date -= ptoffs.BDay(3)
        elif symbol == "NG":
            # https://www.cmegroup.com/trading/energy/natural-gas/natural-gas_contract_specifications.html
            # "Trading terminates on the 3rd last business days of the month
            # prior to the contract month."
            date = datetime.date(year, month, 1)
            # Closes = month preceding the expiry month.
            date += pd.offsets.BMonthEnd(-1)
            # Closes on the "3rd last business days".
            date -= ptoffs.BDay(2)
        else:
            raise NotImplementedError(
                f"Contract lifetime for symbol=`{symbol}` not implemented"
            )

        return imkimetyp.ContractLifetime(
            pd.Timestamp(date - ptoffs.BDay(self.start_timedelta_days)),
            pd.Timestamp(date - ptoffs.BDay(self.end_timedelta_days)),
        )


class FuturesContractLifetimes:
    """
    Read or save a df storing the lifetime of the contracts for a subset of
    symbols.

    The data is organized in directories like:
        - root_dir_name/
            - ContractLifetimeComputer.name/
                - ES.csv
                - CL.csv

    symbol    contract    start_date    end_date
    0    ES    ESZ09    2008-11-20    2009-11-13
    1    ES    ESH10    2009-02-22    2010-02-15
    2    ES    ESM10    2009-05-20    2010-05-13
    """

    def __init__(
        self,
        root_dir_name: str,
        lifetime_computer: ContractLifetimeComputer,
    ) -> None:
        self.root_dir_name = root_dir_name
        self.lifetime_computer = lifetime_computer

    def save(self, symbols: List[str]) -> None:
        """
        Compute the lifetime for all contracts available for all symbols passed
        in.

        :param symbols: Kibot symbols from which to retrieve contracts
        """
        kb = imvkmckime.KibotMetadata()
        hdbg.dassert_type_is(symbols, list)
        hio.create_dir(self.root_dir_name, incremental=True)
        #
        for symbol in tqdm(symbols, desc="Processing futures contract lifetimes"):
            # For each symbol, get all the expiries.
            contracts = kb.get_expiry_contracts(symbol)
            _LOG.debug("Found %s contracts for symbol %s", len(contracts), symbol)
            _LOG.debug("contracts=%s", contracts[0])
            lifetimes = [
                self.lifetime_computer.compute_lifetime(cn) for cn in contracts
            ]
            #
            df = []
            for contract, lifetime in zip(contracts, lifetimes):
                lifetime.start_date = pd.Timestamp(lifetime.start_date)
                lifetime.end_date = pd.Timestamp(lifetime.end_date)
                _LOG.debug(
                    "contract=%s -> [%s, %s]",
                    contract,
                    lifetime.start_date,
                    lifetime.end_date,
                )
                df.append(
                    [symbol, contract, lifetime.start_date, lifetime.end_date]
                )
            df = pd.DataFrame(
                df, columns=["symbol", "contract", "start_date", "end_date"]
            )
            df = df.sort_values(by=["end_date", "start_date"])
            df.reset_index(drop=True, inplace=True)
            # Save.
            file_name = os.path.join(self._get_dir_name(), symbol + ".csv")
            hio.create_enclosing_dir(file_name, incremental=True)
            # hdbg.dassert_file_exist(file_name)
            df.to_csv(file_name)

    def load(self, symbols: List[str]) -> _SymbolToContracts:
        symbol_to_contracts: _SymbolToContracts = {}
        for symbol in symbols:
            file_name = os.path.join(self._get_dir_name(), symbol + ".csv")
            hdbg.dassert_exists(file_name)
            if hs3.is_s3_path(file_name):
                s3fs = hs3.get_s3fs("am")
                kwargs = {"s3fs": s3fs}
            else:
                kwargs = {}
            df = cpanh.read_csv(file_name, index_col=0, **kwargs)
            hdbg.dassert_eq(
                df.columns.tolist(),
                ["symbol", "contract", "start_date", "end_date"],
            )
            for col_name in ["start_date", "end_date"]:
                df[col_name] = pd.to_datetime(df[col_name])
            symbol_to_contracts[symbol] = df
        return symbol_to_contracts

    def _get_dir_name(self) -> str:
        name = self.lifetime_computer.__class__.__name__
        return os.path.join(self.root_dir_name, name)


class FuturesContractExpiryMapper:
    """
    Map symbols to the n-th corresponding contract, based on a certain date.
    """

    def __init__(self, symbol_to_contracts: _SymbolToContracts) -> None:
        # hdbg.dassert_eq(contracts.columns.tolist(), ["symbol", ...])
        self.symbol_to_contracts = symbol_to_contracts

    def get_nth_contract(
        self, symbol: str, date: imkimetyp.DATE_TYPE, n: int
    ) -> Optional[str]:
        """
        Return n-front contract corresponding to a given date and `n` offset.

        :param date: date to use as reference
        :param n: relative month, e.g., 1 for front month, 2 for first back month,
            and so on
        :param symbol: Kibot symbol
        :return: contract in the form of an Expiry
            absolute month and year of contract for `symbol`, expressed using Futures month codes
            and last two digits of year, e.g., `("Z", "20")`
        """
        hdbg.dassert_lte(1, n)
        # Grab all contract lifetimes.
        hdbg.dassert_in(symbol, self.symbol_to_contracts.keys())
        contracts = self.symbol_to_contracts[symbol]
        _LOG.debug("contracts=\n%s", contracts)
        date = pd.Timestamp(date)
        # Find first index with a `start_date` before `date` and
        # an `end_date` after `date`.
        idx = contracts["end_date"].searchsorted(date, side="left")
        if date < contracts["start_date"][idx]:
            # Index does not exist.
            return None
        # Add the offset.
        idx = idx + n - 1
        if idx >= contracts.shape[0]:
            # Index does not exist.
            return None
        # Return the contract.
        ret: str = contracts["contract"][idx]
        return ret

    # TODO(*): Deprecate `get_nth_contract()`.
    def get_nth_contracts(
        self,
        symbol: str,
        start_date: imkimetyp.DATE_TYPE,
        end_date: imkimetyp.DATE_TYPE,
        freq: str,
        n: int,
    ) -> Optional[pd.Series]:
        """
        Return series of nth back contracts from `start_date` to `end_date`.

        :param symbol: contract symbol, e.g., "CL"
        :param start_date: first date/datetime for lookup, inclusive
        :param end_date: last date/datetime for lookup, inclusive
        :param freq: frequency of output series index
        :param n: relative month:
            - 1 for the front month
            - 2 for the first back month
            - etc.
        :return: series of n contract names
        """
        hdbg.dassert_lte(1, n)
        idx = pd.date_range(start=start_date, end=end_date, freq=freq)
        #
        hdbg.dassert_in(symbol, self.symbol_to_contracts.keys())
        contracts = self.symbol_to_contracts[symbol]
        _LOG.debug("contracts=\n%s", contracts)
        # Index contracts by end date.
        contract_end_dates = contracts[["contract", "end_date"]].set_index(
            "end_date"
        )
        hpandas.dassert_strictly_increasing_index(contract_end_dates)
        # Shift to index nth contracts by end date.
        nth_contract_end_dates = contract_end_dates.shift(-1 * (n - 1))
        # Realign the end date to nth contract mapping to `idx` and backfill
        # the contract name.
        # TODO(*): Check for boundary effects.
        nth_contracts = nth_contract_end_dates.reindex(
            idx, method="bfill"
        ).squeeze()
        nth_contracts.name = symbol + str(n)
        return nth_contracts

    def get_contracts(
        self,
        symbols: List[str],
        start_date: imkimetyp.DATE_TYPE,
        end_date: imkimetyp.DATE_TYPE,
        freq: str,
    ) -> Optional[pd.DataFrame]:
        """
        Return dataframe of multiple back contracts.

        :param symbols: list of contract symbols combined with relative month
            for back contract, e.g., ["CL1", "CL2"]
        :param start_date: first date/datetime for lookup, inclusive
        :param end_date: last date/datetime for lookup, inclusive
        :param freq: frequency of output series index
        :return: dataframe of contract names
        """
        contracts = []
        for symbol in symbols:
            contract_name = symbol[:2]
            n = int(symbol[2:])
            srs = self.get_nth_contracts(
                symbol=contract_name,
                start_date=start_date,
                end_date=end_date,
                freq=freq,
                n=n,
            )
            contracts.append(srs)
        df = pd.concat(contracts, axis=1)
        return df
