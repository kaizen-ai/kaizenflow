"""
Import as:

import vendors.kibot.utils as kut
"""

import functools
import logging
import os
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from tqdm import tqdm

import helpers.cache as cache
import helpers.csv as csv
import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors.kibot.data.utils as kdut

# Store a mapping from data frequency (using pandas nomenclature) to the
# column used to compute returns.
RETURN_PRICE_COLS = {"T": "open", "D": "close"}
FIRST_CAUSAL_LAG_PRICE_COLS = {"lhs": "close", "rhs": "open"}

# Top contracts by file size found using
#     kut.read_continuous_contract_metadata().
TOP_KIBOT = {
    "Corn": "C",
    "Crude Oil": "CL",
    "Rough Rice": "RR",
    "Soybeans": "S",
    "Wheat": "W",
    "Copper": "HG",
    "Soybean Meal": "SM",
    "Gold": "GC",
    "Silver": "SI",
    "Palm Oil": "KPO",
}

_LOG = logging.getLogger(__name__)

MEMORY = cache.get_disk_cache(tag=None)

# #############################################################################
# Read data.
# #############################################################################

# The data layout on S3 is:
#
# > aws s3 ls default00-bucket/kibot/
#            PRE All_Futures_Continuous_Contracts_1min/
#            PRE All_Futures_Continuous_Contracts_daily/
#            PRE All_Futures_Continuous_Contracts_tick/
#            PRE All_Futures_Contracts_1min/
#            PRE All_Futures_Contracts_daily/
#            PRE metadata/


# Similar to the Kibot naming style, we refer to:
# - "expiry contracts": futures contract that has a given expiry (e.g., ESH19)
# - "continuous contracts": multiple expiry contracts stitched together, where
#    the prices are always from the front month (e.g., ES)
# - "contracts": both expiry and continuous contracts

# TODO(GPP): I think `cache_data` here is more user-friendly as a boolish
# parameter. Is this ok?
@functools.lru_cache(maxsize=None)
def read_data(
    frequency: str,
    contract_type: str,
    symbols: Union[str, Tuple[str, ...]],
    ext: str = "pq",
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Read kibot data.

    If the ext is `csv`, this function will
    - parse dates
    - add column names
    - check for monotonic index
    For pq data, all of those transformations have already been made,
    so this function reads the data without modifying it.

    :param frequency: `D` or `T` for daily or minutely data respectively
    :param contract_type: `continuous` or `expiry`
    :param symbols: symbol or tuple of symbols
    :param ext: whether to read `pq` or `csv` data
    :param nrows: if not None, return only the first nrows of the data
    :param cache_data: whether to use cached data if exists
    :return: if `symbols` is a string, return pd.DataFrame with kibot
        data. If `symbols` is a list, return a dictionary of dataframes
        for each symbol.
    """
    if isinstance(symbols, str):
        data = _read_single_symbol_data(
            frequency, contract_type, symbols, ext, nrows, cache_data
        )
    elif isinstance(symbols, tuple):
        data = _read_multiple_symbol_data(
            frequency, contract_type, symbols, ext, nrows, cache_data
        )
    else:
        raise ValueError("Invalid type(symbols)=%s" % type(symbols))
    return data


# #############################################################################
# Read metadata.
# #############################################################################


_KIBOT_DIRNAME = os.path.join(hs3.get_path(), "kibot/metadata")


class KibotMetadata:
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
        return self.get_metadata(contract_type).index.tolist()

    @staticmethod
    def get_expiry_contracts(symbol: str) -> List[str]:
        """
        Return the expiry contracts corresponding to a continuous contract.
        """
        one_min_contract_metadata = read_1min_contract_metadata()
        one_min_contract_metadata, _ = KibotMetadata._extract_month_year_expiry(
            one_min_contract_metadata
        )
        # Select the rows with the Symbol equal to the requested one.
        mask = one_min_contract_metadata["SymbolBase"] == symbol
        df = one_min_contract_metadata[mask]
        contracts = df.loc[:, "Symbol"].tolist()
        return contracts

    # //////////////////////////////////////////////////////////////////////////

    # TODO(Julia): Replace `one_min` with `expiry` once the PR is approved.
    @staticmethod
    def _compute_kibot_metadata(contract_type: str) -> pd.DataFrame:
        if contract_type in ["1min", "daily"]:
            # Minutely and daily dataframes are identical except for the `Link`
            # column.
            one_min_contract_metadata = read_1min_contract_metadata()
        elif contract_type == "tick-bid-ask":
            one_min_contract_metadata = read_tickbidask_contract_metadata()
        else:
            raise ValueError("Invalid `contract_type`='%s'" % contract_type)
        continuous_contract_metadata = read_continuous_contract_metadata()
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
        kibot_metadata = pd.concat(to_concat, axis=1, join="outer", sort=True,)
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
            .apply(ExpiryContractMapper.parse_expiry_contract)
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
        :return: Kibot metadata annotated with exchange mappings
        """
        kibot_to_cme_mapping = kdut.read_kibot_exchange_mapping()
        # Add mapping columns to the dataframe.
        annotated_metadata = pd.concat(
            [kibot_metadata, kibot_to_cme_mapping], axis=1
        )
        return annotated_metadata


class ContractSymbolMapping:
    def __init__(self) -> None:
        km = KibotMetadata()
        # Minutely and daily dataframes are identical except for the `Link`
        # column, so it does not matter, which one we load.
        self._metadata = km.get_metadata("1min")

    def get_contract(self, symbol: str) -> Optional[str]:
        """
        Get contract for Kibot symbol.

        :param symbol: value for `Kibot_symbol` column
        :return: `Exchange_abbreviation`:`Exchange_symbol` format contract
        """
        contract_metadata = self._metadata[
            self._metadata["Kibot_symbol"] == symbol
        ]
        if contract_metadata.empty:
            _LOG.warning("No metadata for %s symbol", symbol)
            return
        dbg.dassert_eq(contract_metadata.shape[0], 1)
        exchange = contract_metadata["Exchange_abbreviation"].iloc[0]
        if exchange is None:
            _LOG.warning("Exchange is `None` for %s symbol", symbol)
            return None
        exchange_symbol = contract_metadata["Exchange_symbol"].iloc[0]
        if exchange_symbol is None:
            _LOG.warning("Exchange symbol is `None` for %s symbol", symbol)
            return None
        return f"{exchange}:{exchange_symbol}"

    def get_kibot_symbol(self, contract: str) -> Optional[Union[str, List[str]]]:
        """
        Get Kibot symbol for contract.

        :param contract: `Exchange_abbreviation`:`Exchange_symbol`
        :return: Kibot symbol
        """
        contract_split = contract.split(":")
        dbg.dassert_eq(len(contract_split), 2)
        exchange, exchange_symbol = contract_split
        contract_metadata = self._metadata[
            (self._metadata["Exchange_abbreviation"] == exchange)
            & (self._metadata["Exchange_symbol"] == exchange_symbol)
        ]
        if contract_metadata.empty:
            _LOG.warning("No metadata for %s contract", contract)
            return
        # Seven exchange symbols are mapped to multiple contracts:
        # https://github.com/ParticleDev/commodity_research/issues/2988#issuecomment-646351846.
        # TODO(Julia): Once the mapping is fixed, remove this.
        if contract_metadata.shape[0] > 1:
            if exchange_symbol == "T":
                # There are two contracts with this `Exchage_symbol`:
                # `CONTINUOUS UK FEED WHEAT CONTRACT` and
                # `CONTINUOUS WTI CRUDE CONTRACT`. Return WTI.
                return "CRD"
            elif exchange_symbol == "C":
                # There are two contracts with
                # `CONTINUOUS EUA CONTRACT` and
                # `CONTINUOUS LONDON COCOA CONTRACT`. Return EUA.
                return "UX"
            else:
                return contract_metadata["Kibot_symbol"].tolist()
        kibot_symbol = contract_metadata["Kibot_symbol"].iloc[0]
        return kibot_symbol


# TODO(gp): Might make sense to cache the metadata too to avoid s3 access.
def read_1min_contract_metadata() -> pd.DataFrame:
    """
    Read minutely contract metadata.

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
    file_name = _KIBOT_DIRNAME + "/All_Futures_Contracts_1min.csv.gz"
    _LOG.debug("file_name=%s", file_name)
    df = pd.read_csv(file_name, index_col=0)
    df = df.iloc[:, 1:]
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


def read_daily_contract_metadata() -> pd.DataFrame:
    """
    Read daily contract metadata.

    Same mapping as `read_1min_contract_metadata()` but for daily prices.

    Columns are:
    - Symbol
    - Link
    - Description

    Symbol    Link                                                Description
    JY        http://api.kibot.com/?action=download&link=151...   CONTINUOUS JAPANESE YEN CONTRACT
    JYF18    http://api.kibot.com/?action=download&link=vrv...    JAPANESE YEN JANUARY 2018

    """
    file_name = _KIBOT_DIRNAME + "/All_Futures_Contracts_daily.csv.gz"
    _LOG.debug("file_name=%s", file_name)
    df = pd.read_csv(file_name, index_col=0)
    df = df.iloc[:, 1:]
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("df.shape=%s", df.shape)
    return df


def read_tickbidask_contract_metadata() -> pd.DataFrame:
    """
    Read tick-bid-ask contract metadata.

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
    file_name = _KIBOT_DIRNAME + "/Futures_tickbidask.txt.gz"
    _LOG.debug("file_name=%s", file_name)
    df = pd.read_csv(file_name, index_col=0, skiprows=5, header=None, sep="\t")
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


def read_continuous_contract_metadata() -> pd.DataFrame:
    """
    Read tick-bid-ask metadata for continuous contracts.

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
    file_name = _KIBOT_DIRNAME + "/FuturesContinuous_intraday.txt.gz"
    _LOG.debug("file_name=%s", file_name)
    df = pd.read_csv(file_name, index_col=0, skiprows=5, header=None, sep="\t")
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


class ExpiryContractMapper:
    """
    Implement functions to handle expiry contracts, e.g., "ESH19".
    """

    def month_to_expiry(self, month: str) -> str:
        dbg.dassert_in(month, self._MONTH_TO_EXPIRY)
        return self._MONTH_TO_EXPIRY[month]

    def expiry_to_month(self, expiry: str) -> str:
        dbg.dassert_in(expiry, self._EXPIRY_TO_MONTH)
        return self._EXPIRY_TO_MONTH[expiry]

    @staticmethod
    def parse_expiry_contract(v: str) -> Tuple[str, str, str]:
        """
        Parse a futures contract name into its components, e.g., in a futures
        contract name like "ESH10":
            - base symbol is ES
            - month is H
            - year is 10 (i.e., 2010)
        """
        m = re.match(r"^(\S+)(\S)(\d{2})$", v)
        dbg.dassert(m, "Invalid '%s'", v)
        base_symbol, month, year = m.groups()
        return base_symbol, month, year

    @staticmethod
    def compare_expiry_contract(v1: str, v2: str) -> int:
        """
        Compare function for two expiry contracts in terms of month and year (
        e.g., "U10") according to python `cmp` convention.

        :param: return -1, 0, 1 in case of <, ==, > relationship between v1 and
            v2.
        """
        base_symbol1, month1, year1 = ExpiryContractMapper.parse_expiry_contract(
            v1
        )
        base_symbol2, month2, year2 = ExpiryContractMapper.parse_expiry_contract(
            v2
        )
        dbg.dassert_eq(base_symbol1, base_symbol2)
        # Compare.
        if year1 < year2 or (year1 == year2 and month1 < month2):
            res = -1
        elif year1 == year2 and month1 == month2:
            res = 0
        else:
            res = 1
        return res

    @staticmethod
    def sort_expiry_contract(contracts: Iterable[str]) -> Iterable[str]:
        # python3 removed `cmp` from sort so we need to convert it into a key.
        contracts = sorted(
            contracts,
            key=functools.cmp_to_key(
                ExpiryContractMapper.compare_expiry_contract
            ),
        )
        return contracts

    # //////////////////////////////////////////////////////////////////////////

    _MONTH_TO_EXPIRY = {
        "January": "F",
        "February": "G",
        "March": "H",
        "April": "J",
        "May": "K",
        "June": "M",
        "July": "N",
        "August": "Q",
        "September": "U",
        "October": "V",
        "November": "X",
        "December": "Z",
    }

    _EXPIRY_TO_MONTH = {v: k for k, v in _MONTH_TO_EXPIRY.items()}


# #############################################################################
# Convert .csv.gz to parquet.
# #############################################################################


def convert_kibot_csv_gz_to_pq() -> None:
    """
    Convert the files in the following subdirs of `kibot` dir on S3:
    - `All_Futures_Contracts_1min`
    - `All_Futures_Continuous_Contracts_1min`
    - `All_Futures_Contracts_daily`
    - `All_Futures_Continuous_Contracts_daily`
    to Parquet and save them to `kibot/pq` dir on S3.
    """
    # Get the list of kibot subdirs in the kibot directory on S3.
    s3_path = hs3.get_path()
    kibot_dir_path = os.path.join(s3_path, "kibot")
    kibot_subdirs = hs3.listdir(kibot_dir_path, mode="non-recursive")
    _LOG.debug("Convert files in the following dirs: %s", kibot_subdirs)
    # For each of the kibot subdirectories, transform the files in them
    # to parquet.
    pq_dir = os.path.join(kibot_dir_path, "pq")
    for kibot_subdir in tqdm(iter(kibot_subdirs)):
        csv_subdir_path = os.path.join(kibot_dir_path, kibot_subdir)
        _convert_kibot_subdir_csv_gz_to_pq(csv_subdir_path, pq_dir)


# #############################################################################
# Private helpers.
# #############################################################################


def _convert_kibot_subdir_csv_gz_to_pq(csv_subdir_path: str, pq_dir: str) -> None:
    csv_subdir_path = csv_subdir_path.rstrip("/")
    kibot_subdir = os.path.basename(csv_subdir_path)
    _LOG.info("Converting files in %s directory", csv_subdir_path)
    normalizer = _get_normalizer(kibot_subdir)
    if normalizer is None:
        _LOG.warning("Skipping dir '%s'", kibot_subdir)
        return
    pq_subdir_path = os.path.join(pq_dir, kibot_subdir)
    csv.convert_csv_dir_to_pq_dir(
        csv_subdir_path, pq_subdir_path, header=None, normalizer=normalizer
    )
    _LOG.info(
        "Converted the files in %s directory and saved them to %s.",
        csv_subdir_path,
        pq_subdir_path,
    )


def _get_kibot_path(
    frequency: str, contract_type: str, symbol: str, ext: str = "pq"
) -> str:
    """
    Get the path to a specific kibot dataset on s3.

    Parameters as in `read_data`.
    :return: path to the file
    """
    if frequency == "T":
        freq_path = "1min"
    elif frequency == "D":
        freq_path = "daily"
    else:
        raise ValueError("Invalid frequency='%s'" % frequency)
    if contract_type == "continuous":
        contract_path = "_Continuous"
    elif contract_type == "expiry":
        contract_path = ""
    else:
        raise ValueError("Invalid contract_type='%s'" % contract_type)
    dir_name = f"All_Futures{contract_path}_Contracts_{freq_path}"
    file_path = os.path.join(dir_name, symbol)
    if ext == "pq":
        # Parquet files are located in `pq/` subdirectory.
        file_path = os.path.join("pq", file_path)
        file_path += ".pq"
    elif ext == "csv":
        file_path += ".csv.gz"
    else:
        raise ValueError("Invalid ext='%s" % ext)
    file_path = os.path.join(hs3.get_path(), "kibot", file_path)
    return file_path


def _read_multiple_symbol_data(
    frequency: str,
    contract_type: str,
    symbols: Tuple[str, ...],
    ext: str = "pq",
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> Dict[str, pd.DataFrame]:
    return {
        symbol: _read_single_symbol_data(
            frequency, contract_type, symbol, ext, nrows, cache_data
        )
        for symbol in symbols
    }


def _read_single_symbol_data(
    frequency: str,
    contract_type: str,
    symbol: str,
    ext: str = "pq",
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> pd.DataFrame:
    file_path = _get_kibot_path(frequency, contract_type, symbol, ext)
    if cache_data:
        data = _read_data_from_disk_cache(file_path, nrows)
    else:
        data = _read_data_from_disk(file_path, nrows)
    return data


@MEMORY.cache
def _read_data_from_disk_cache(
    file_path: str, nrows: Optional[int]
) -> pd.DataFrame:
    data = _read_data(file_path, nrows)
    return data


def _read_data_from_disk(file_path: str, nrows: Optional[int]) -> pd.DataFrame:
    data = _read_data(file_path, nrows)
    return data


def _read_data(file_path: str, nrows: Optional[int]) -> pd.DataFrame:
    ext = _split_multiple_ext(file_path)[-1]
    if ext == ".pq":
        # Read the data.
        df = pd.read_parquet(file_path)
        if nrows is not None:
            df = df.head(nrows)
    elif ext == ".csv.gz":
        # Read and normalize the data.
        # In the parquet flow we have already applied all the
        # transformations, while in the csv.gz flow we apply the
        # transformation on the raw data that comes from Kibot.
        df = pd.read_csv(file_path, header=None, nrows=nrows)
        dir_name = os.path.basename(os.path.dirname(file_path))
        normalizer = _get_normalizer(dir_name)
        if normalizer is not None:
            df = normalizer(df)
        else:
            raise ValueError(
                "Invalid dir_name='%s' in file_name='%s'" % (dir_name, file_path)
            )
    else:
        raise ValueError("Invalid extension='%s'" % ext)
    return df


# TODO(J): Move this function to `helpers.io_`.
def _split_multiple_ext(file_name: str) -> Tuple[str, str]:
    """
    Split file name into root and extension. Extension is everything
    after the first dot, ignoring the leading dot.

    The difference with `os.path.splitext` is that this function assumes
    that extension starts after the first dot, not the last. Therefore,
    if `file_name='file.csv.gz'`, this function will extract `.csv.gz`
    extension, while `os.path.splitext` will extract only `.gz`.

    :param file_name: the name of the file
    :return: root of the file name and extension
    """
    file_name_without_ext = file_name
    while True:
        file_name_without_ext, ext = os.path.splitext(file_name_without_ext)
        if ext == "":
            break
    full_ext = file_name.replace(file_name_without_ext, "", 1)
    return file_name_without_ext, full_ext


# TODO(gp): Call the column datetime_ET suffix.
def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a df with 1 min Kibot data into our internal format.

    - Combine the first two columns into a datetime index
    - Add column names
    - Check for monotonic index

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe with `datetime` index and `open`, `high`,
        `low`, `close`, `vol`, `time` columns. If the input dataframe
        has only one column, the column name will be transformed to
        string format.
    """
    # There are cases in which the dataframes consist of only one column,
    # with the first row containing a `405 Data Not Found` string, and
    # the second one containing `No data found for the specified period
    # for BTSQ14.`
    if df.shape[1] > 1:
        # According to Kibot the columns are:
        #   Date,Time,Open,High,Low,Close,Volume
        # Convert date and time into a datetime.
        df[0] = pd.to_datetime(df[0] + " " + df[1], format="%m/%d/%Y %H:%M")
        df.drop(columns=[1], inplace=True)
        # Rename columns.
        df.columns = "datetime open high low close vol".split()
        df.set_index("datetime", drop=True, inplace=True)
        _LOG.debug("Add columns")
        df["time"] = [d.time() for d in df.index]
    else:
        df.columns = df.columns.astype(str)
        _LOG.warning("The dataframe has only one column: %s", df)
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


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


def _get_normalizer(kibot_subdir: str) -> Optional[Callable]:
    """
    Chose a normalizer function based on a directory name.

    :param kibot_subdir: directory name
    :return: `_normalize_1_min`, `_normalize_daily` or None
    """
    if kibot_subdir in [
        "All_Futures_Contracts_1min",
        "All_Futures_Continuous_Contracts_1min",
    ]:
        # 1 minute data.
        normalizer: Optional[Callable] = _normalize_1_min
    elif kibot_subdir in [
        "All_Futures_Continuous_Contracts_daily",
        "All_Futures_Contracts_daily",
    ]:
        # Daily data.
        normalizer = _normalize_daily
    else:
        normalizer = None
    return normalizer


def _get_file_name(file_name: str, mode: str = "pq") -> str:
    file_name_without_ext, ext = _split_multiple_ext(file_name)
    if mode == "pq":
        if ext != ".pq":
            _LOG.warning("mode='%s', extension='%s'. Reading pq data.", mode, ext)
            file_name_without_ext = file_name_without_ext.replace(
                "kibot/", "kibot/pq/"
            )
            file_name = file_name_without_ext + ".pq"
    elif mode == "csv":
        if ext != ".csv.gz":
            _LOG.warning(
                "mode='%s', extension='%s'. Reading .csv.gz data.", mode, ext
            )
            file_name_without_ext = file_name_without_ext.replace(
                "kibot/pq/", "kibot/"
            )
            file_name = file_name_without_ext + ".csv.gz"
    else:
        raise ValueError("Invalid mode='%s" % mode)
    return file_name


# #############################################################################
# Transform.
# #############################################################################


# Kibot documentation (from http://www.kibot.com/Support.aspx#data_format)
# states the following timing semantic:
#    "a time stamp of 10:00 AM is for a period between 10:00:00 AM and 10:00:59 AM"
#    "All records with a time stamp between 9:30:00 AM and 3:59:59 PM represent
#    the regular US stock market trading session."
#
# Thus the open price at time "ts" corresponds to the instantaneous price at
# time "ts", which by Alphamatic conventions corresponds to the "end" of an
# interval in the form [a, b) interval

# As a consequence our usual "ret_0" # (price entering instantaneously at time
# t - 1 and exiting at time t) is implemented in terms of Kibot data as:
#   ret_0(t) = open_price(t) - open_price(t - 1)
#
#              datetime     open     high      low    close   vol      time  ret_0
# 0 2009-09-27 18:00:00  1042.25  1043.25  1042.25  1043.00  1354  18:00:00    NaN
# 1 2009-09-27 18:01:00  1043.25  1043.50  1042.75  1042.75   778  18:01:00   1.00
#
# E.g., ret_0(18:01) is the return realized entering (instantaneously) at 18:00
# and exiting at 18:01
#
# In reality we need time to:
# - compute the forecast
# - enter the position
# We can't use open at time t - 1 since this would imply instantaneous forecast
# We can use data at time t - 2, which corresponds to [t-1, t-2), although
# still we would need to enter instantaneously A better assumption is to let 1
# minute to enter in position, so:
# - use data for [t - 2, t - 1) (which Kibot tags with t - 2)
# - enter in position between [t - 1, t)
# - capture the return realized between [t, t + 1]
# In other terms we need 1 extra delay (probably 2 would be even safer)
