import abc
import re
from typing import Any, List, Tuple, Optional, Type, Dict

import pandas as pd
import helpers.dbg as dbg

import vendors2.kibot.metadata.load.expiry_contract_mapper as vkmlex
import vendors2.kibot.data.load.s3_data_loader as vkdls3
import vendors2.kibot.metadata.load.s3_backend as vkmls3
import vendors2.kibot.metadata.load.expiry_contract_mapper as vkmdle
import vendors2.kibot.data.types as vkdt
import vendors2.kibot.metadata.types as vkmdt


class ContractLifetimeComputer(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def compute_lifetime(contract_name: str) -> vkmdt.ContractLifetime:
        """Compute the lifetime of a contract, e.g. 'CLJ17'.

        :param contract_name: the contract for which to compute the lifetime.
        :return: the computed lifetime.
        """


class KibotTradingActivityContractLifetimeComputer(ContractLifetimeComputer):
    @staticmethod
    def compute_lifetime(contract_name: str) -> vkmdt.ContractLifetime:
        """Use the price data from Kibot to compute the lifetime."""
        df = vkdls3.S3KibotDataLoader()\
            .read_data("Kibot", contract_name,
                       vkdt.AssetClass.Futures, vkdt.Frequency.Daily,
                       vkdt.ContractType.Expiry)
        start_date = pd.Timestamp(df.first_valid_index())
        end_date = pd.Timestamp(df.last_valid_index())
        return vkmdt.ContractLifetime(start_date, end_date)


class Contract:
    def __init__(self, contract_name: str, compute_backend: Type[ContractLifetimeComputer]):
        self._contract_name = contract_name
        self._compute_backend = compute_backend
        self._lifetime: Optional[vkmdt.ContractLifetime] = None

    def get_lifetime(self) -> vkmdt.ContractLifetime:
        """Get the lifetime of this contract.

        `get_lifetime` returns from memory. If the lifetime hasn't been computed,
        it will compute it & store it in memory before returning.

        :return: the lifetime of this contract.
        """
        if self._lifetime is None:
            self.compute_lifetime()
        return self._lifetime

    def compute_lifetime(self) -> None:
        """Uses the provided compute backend, to compute the lifetime.

        After computing the lifetime, it stores it in memory.
        """
        self._lifetime = self._compute_backend.compute_lifetime(self._contract_name)


class ContractExpiryMapper:
    def __init__(self, symbols: List[str], lifetime_computer: Type[ContractLifetimeComputer]):
        """

        :param symbols: symbols to retrieve the contracts from
        :param lifetime_computer: computer that will compute the lifetime
        """
        kb = KibotMetadata()
        # symbols = kb.get_kibot_symbols()
        self.contracts: Dict[str, List[Contract]] = {}
        for symbol in symbols:
            contracts = [Contract(cn, lifetime_computer) for cn in kb.get_expiry_contracts(symbol)]
            self.contracts[symbol] = contracts

    def get_expiry(self, date: vkmdt.DATE_TYPE, date_month_offset: int, symbol: str) -> Optional[vkmdt.Expiry]:
        """Return expiry for contract given `datetime` and `month` offset.

        :param date: includes year, month, day, and possibly time (otherwise ... assumed)
        :param date_month_offset: relative month, e.g., 1 for front month, 2 for first back month, and so on
        :param symbol: Kibot symbol
        :return: absolute month and year of contract for `symbol`, expressed using Futures month codes
            and last two digits of year, e.g., `("Z", "20")`
        """
        dbg.dassert_in(symbol, self.contracts.keys())
        df = pd.DataFrame(columns=['contract', 'start_date', 'end_date'])
        # grab all contract lifetimes
        for contract in self.contracts[symbol]:
            contract_lifetime = contract.get_lifetime()
            index = df['end_date'].searchsorted(contract_lifetime.end_date)
            row = pd.DataFrame({'contract': contract,
                                'start_date': contract_lifetime.start_date,
                                'end_date': contract_lifetime.end_date}, index=[index])
            df = pd.concat([df.iloc[:index], row, df.iloc[index:]]).reset_index(drop=True)

        # find first index with a `start_date` before `date` and
        # an `end_date` after `date`
        idx = df['end_date'].searchsorted(pd.Timestamp(date), side='left')
        while df['start_date'].iloc[idx] > date:
            idx = df['end_date'].searchsorted(pd.Timestamp(date), side='left')
            # 0 = no contracts with a `start_date` before `date`
            if idx >= len(df.index) or idx == 0:
                return None

        # add the offset
        idx = idx + date_month_offset
        if idx >= len(df.index):
            # index does not exist
            return None

        # return the expiry date
        ret = df['end_date'][idx]
        return vkmdt.Expiry(
            month=vkmdle.ExpiryContractMapper().month_to_expiry_num(ret.month),
            year=str(ret.year)[2::]
        )


class KibotMetadata:
    # pylint: disable=line-too-long
    """
    Generate Kibot metadata.

    The metadata is computed from:
     - minutely contract metadata (`read_1min_contract_metadata()`)
     - tick-bid-ask metadata (`read_continuous_contract_metadata()`) is used to
       extract start date and exchange, which are not available in the minutely
       metadata.

    The expiration dates provided here are accurate for both daily and minutely
    metadata.

    The metadata is indexed by the symbol.

    The metadata contains the following columns:
    - `Description`
    - `StartDate`
    - `Exchange`
    - `num_contracts`
    - `min_contract`
    - `max_contract`
    - `num_expiries`
    - `expiries`

                                   Description  StartDate                                  Exchange  num_contracts min_contract max_contract  num_expiries                                expiries
    AD   CONTINUOUS AUSTRALIAN DOLLAR CONTRACT  9/27/2009  Chicago Mercantile Exchange (CME GLOBEX)           65.0      11.2009      11.2020          12.0  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    AEX          CONTINUOUS AEX INDEX CONTRACT        NaN                                       NaN          116.0      03.2010      02.2020          12.0  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    """
    # pylint: enable=line-too-long

    def __init__(self) -> None:
        self.minutely_metadata = self._compute_kibot_metadata("1min")
        self.tickbidask_metadata = self._compute_kibot_metadata("tick-bid-ask")

    def get_metadata(self, contract_type: str = "1min") -> pd.DataFrame:
        """
        Return the metadata.
        """
        if contract_type in ["1min", "daily"]:
            # Minutely and daily dataframes are identical except for the `Link`
            # column.
            metadata = self.minutely_metadata.copy()
        elif contract_type == "tick-bid-ask":
            metadata = self.tickbidask_metadata.copy()
        else:
            raise ValueError("Invalid `contract_type`='%s'" % contract_type)
        return metadata

    def get_futures(self, contract_type: str = "1min") -> List[str]:
        """
        Return the continuous contracts, e.g., ES, CL.
        """
        futures: List[str] = self.get_metadata(contract_type).index.tolist()
        return futures

    def get_kibot_symbols(self, contract_type: str = "1min") -> pd.Series:
        metadata = self.get_metadata(contract_type)
        return metadata['Kibot_symbol']

    @staticmethod
    def get_expiry_contracts(symbol: str) -> List[str]:
        """
        Return the expiry contracts corresponding to a continuous contract.
        """
        one_min_contract_metadata = vkmls3.S3Backend().read_1min_contract_metadata()
        one_min_contract_metadata, _ = KibotMetadata._extract_month_year_expiry(
            one_min_contract_metadata
        )
        # Select the rows with the Symbol equal to the requested one.
        mask = one_min_contract_metadata["SymbolBase"] == symbol
        df = one_min_contract_metadata[mask]
        contracts: List[str] = df.loc[:, "Symbol"].tolist()
        return contracts

    # //////////////////////////////////////////////////////////////////////////

    # TODO(Julia): Replace `one_min` with `expiry` once the PR is approved.
    @staticmethod
    def _compute_kibot_metadata(contract_type: str) -> pd.DataFrame:
        s3_backend = vkmls3.S3Backend()
        if contract_type in ["1min", "daily"]:
            # Minutely and daily dataframes are identical except for the `Link`
            # column.
            one_min_contract_metadata = s3_backend.read_1min_contract_metadata()
        elif contract_type == "tick-bid-ask":
            one_min_contract_metadata = (
                s3_backend.read_tickbidask_contract_metadata()
            )
        else:
            raise ValueError("Invalid `contract_type`='%s'" % contract_type)
        continuous_contract_metadata = (
            s3_backend.read_continuous_contract_metadata()
        )
        # Extract month, year, expiries and SymbolBase from the Symbol col.
        (
            one_min_contract_metadata,
            one_min_symbols_metadata,
        ) = KibotMetadata._extract_month_year_expiry(one_min_contract_metadata)
        # Calculate stats for expiries.
        expiry_counts = KibotMetadata._calculate_expiry_counts(
            one_min_contract_metadata
        )
        # Drop unneeded columns from the symbol metadata dataframe
        # originating from 1 min contract metadata.
        one_min_contracts = one_min_symbols_metadata.copy()
        one_min_contracts.set_index("Symbol", inplace=True)
        one_min_contracts.drop(
            columns=["year", "Link"], inplace=True, errors="ignore"
        )
        # Choose needed columns from the continuous contract metadata.
        cont_contracts_chosen = continuous_contract_metadata.loc[
            :, ["Symbol", "StartDate", "Exchange"]
        ]
        cont_contracts_chosen = cont_contracts_chosen.set_index(
            "Symbol", drop=True
        )
        # Combine 1 min metadata, continuous contract metadata and stats for
        # expiry contracts.
        if contract_type == "tick-bid-ask":
            to_concat = [one_min_contracts, expiry_counts]
        else:
            to_concat = [one_min_contracts, cont_contracts_chosen, expiry_counts]
        kibot_metadata = pd.concat(
            to_concat,
            axis=1,
            join="outer",
            sort=True,
        )
        # Sort by index.
        kibot_metadata.sort_index(inplace=True)
        # Remove empty nans.
        kibot_metadata.dropna(how="all", inplace=True)
        # Convert date columns to datetime.
        kibot_metadata["min_contract"] = pd.to_datetime(
            kibot_metadata["min_contract"], format="%m.%Y"
        )
        kibot_metadata["max_contract"] = pd.to_datetime(
            kibot_metadata["max_contract"], format="%m.%Y"
        )
        # Convert integer columns to `int`.
        kibot_metadata["num_contracts"] = kibot_metadata["num_contracts"].astype(
            int
        )
        kibot_metadata["num_expiries"] = kibot_metadata["num_expiries"].astype(
            int
        )
        # Append Exchange_symbol, Exchange_group, Globex_symbol columns.
        kibot_metadata = KibotMetadata._annotate_with_exchange_mapping(
            kibot_metadata
        )
        # Change index to continuous.
        kibot_metadata = kibot_metadata.reset_index()
        kibot_metadata = kibot_metadata.rename({"index": "Kibot_symbol"}, axis=1)
        columns = [
            "Kibot_symbol",
            "Description",
            "StartDate",
            "Exchange",
            "Exchange_group",
            "Exchange_abbreviation",
            "Exchange_symbol",
            "num_contracts",
            "min_contract",
            "max_contract",
            "num_expiries",
            "expiries",
        ]
        return kibot_metadata[columns]

    _CONTRACT_EXPIRIES = {
        "F": 1,
        "G": 2,
        "H": 3,
        "J": 4,
        "K": 5,
        "M": 6,
        "N": 7,
        "Q": 8,
        "U": 9,
        "V": 10,
        "X": 11,
        "Z": 12,
    }

    @staticmethod
    def _get_zero_elememt(list_: List[Any]) -> Any:
        return list_[0] if list_ else None

    @staticmethod
    def _extract_month_year_expiry(
        one_min_contract_metadata: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract month, year, expiries and SymbolBase from the Symbol.
        """
        # Extract year by extracting the trailing digits. Contracts that
        # do not have a year are continuous.
        one_min_contract_metadata = one_min_contract_metadata.copy()
        one_min_contract_metadata["year"] = (
            one_min_contract_metadata["Symbol"]
            .apply(lambda x: re.findall(r"\d+$", x))
            .apply(KibotMetadata._get_zero_elememt)
        )
        one_min_symbols_metadata = one_min_contract_metadata.loc[
            one_min_contract_metadata["year"].isna()
        ]
        # Drop continuous contracts.
        one_min_contract_metadata.dropna(subset=["year"], inplace=True)
        # Extract SymbolBase, month, year and expiries from contract names.
        symbol_month_year = (
            one_min_contract_metadata["Symbol"]
            .apply(vkmlex.ExpiryContractMapper.parse_expiry_contract)
            .apply(pd.Series)
        )
        symbol_month_year.columns = ["SymbolBase", "month", "year"]
        symbol_month_year["expiries"] = (
            symbol_month_year["month"] + symbol_month_year["year"]
        )
        symbol_month_year.drop(columns="year", inplace=True)
        one_min_contract_metadata.drop(
            columns="SymbolBase", inplace=True, errors="ignore"
        )
        one_min_contract_metadata = pd.concat(
            [one_min_contract_metadata, symbol_month_year], axis=1
        )
        return one_min_contract_metadata, one_min_symbols_metadata

    @staticmethod
    def _calculate_expiry_counts(
        one_min_contract_metadata: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate the following stats for each symbol:

        - number of contracts
        - number of expiries
        - the oldest contract
        - the newest contract

        :return: pd.DataFrame with calculated counts
        """
        one_min_contracts_with_exp = one_min_contract_metadata.copy()
        # To sort the contracts easily, revert expiries so that the year
        # comes before month.
        one_min_contracts_with_exp[
            "expiries_year_first"
        ] = one_min_contracts_with_exp["expiries"].apply(lambda x: x[1:] + x[0])
        base_groupby = one_min_contracts_with_exp.groupby("SymbolBase")
        # Count the contracts.
        num_contracts = pd.Series(
            base_groupby["expiries"].nunique(), name="num_contracts"
        )
        # Get months at which the contract expires.
        num_expiries = pd.Series(
            base_groupby["month"].nunique(), name="num_expiries"
        )
        # Get the earliest contract, bring it to the mm.yyyy format.
        min_contract = pd.Series(
            base_groupby["expiries_year_first"].min(), name="min_contract"
        )
        min_contract = min_contract.apply(
            lambda x: str(KibotMetadata._CONTRACT_EXPIRIES[x[-1]]).zfill(2)
            + ".20"
            + x[:2]
        )
        # Get the oldest contract, bring it to the mm.yyyy format.
        max_contract = pd.Series(
            base_groupby["expiries_year_first"].max(), name="max_contract"
        )
        max_contract = max_contract.apply(
            lambda x: str(KibotMetadata._CONTRACT_EXPIRIES[x[-1]]).zfill(2)
            + ".20"
            + x[:2]
        )
        # Get all months at which contracts for each symbol expires,
        # change the str months to the month numbers from 0 to 11.
        expiries = pd.Series(base_groupby["month"].unique(), name="expiries")
        expiries = expiries.apply(
            lambda x: list(map(lambda y: KibotMetadata._CONTRACT_EXPIRIES[y], x))
        )
        # Combine all counts.
        expiry_counts = pd.concat(
            [num_contracts, min_contract, max_contract, num_expiries, expiries],
            axis=1,
        )
        return expiry_counts

    @staticmethod
    def _annotate_with_exchange_mapping(
        kibot_metadata: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Annotate Kibot with exchanges and their symbols.

        The annotations include
         - "Exchange_group" for high-level exchanges' group
         - "Exchange_abbreviation" for exchange abbreviation
         - "Exchange_symbol" for contract designation in given exchange

        Annotations are provided only for commodity-related contracts.

        :param kibot_metadata: Kibot metadata dataframe
        kibot_to_cme_mapping = (
            vkmls3.S3Backend().read_kibot_exchange_mapping()
        )
        """
        kibot_to_cme_mapping = vkmls3.S3Backend().read_kibot_exchange_mapping()
        # Add mapping columns to the dataframe.
        annotated_metadata = pd.concat(
            [kibot_metadata, kibot_to_cme_mapping], axis=1
        )
        return annotated_metadata
