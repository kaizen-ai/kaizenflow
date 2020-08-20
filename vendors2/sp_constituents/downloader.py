#!/usr/bin/env python
"""
Download current SP* constituents list and its historical changes in csv.

Import as:
import vendors2.sp_constituents.downloader as sp_dl

Use example:
# Download SP* in the default folder 'vendors2/sp_constituents/data':
> downloader.py
"""

import argparse
import logging
import os
from typing import List, Optional, Tuple

import pandas as pd

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


class _SP_Downloader:
    def __init__(self, prefix: str, wiki_url: str) -> None:
        """
        Initialize the SP*_Downloader by setting proper file prefix and URL to a Wiki page.

        :param prefix: prefix for the constituents list, e.g., SP500, SP400
        :param wiki_url: URL to a Wiki page
        """
        self._prefix = prefix
        self._wiki_url = wiki_url

    def get_file_paths(
        self, dir_name: Optional[str] = None, raw: bool = False
    ) -> Tuple[str, str]:
        """
        Get full paths to the csv files with the constituents lists.

        :param raw: flag if data is raw from the Wiki
        :param dir_name: directory with the data
        :return: full paths to the current state of constituents and its historical changes
        """
        root_dir = git.get_client_root(True)
        prefix = self._prefix
        suffix = ".raw" if raw else ""
        dir_name = dir_name if dir_name else "vendors2/sp_constituents/data"
        dst_dir = os.path.join(root_dir, dir_name)
        path_current = os.path.join(
            dst_dir, prefix + ".current_state" + suffix + ".csv"
        )
        path_changes = os.path.join(
            dst_dir, prefix + ".historical_changes" + suffix + ".csv"
        )
        return path_current, path_changes

    def download_raw_data(self) -> None:
        """
        Download and parse SP* information from the data source.

        :return: current state of SP* constituents and its historical changes
        """
        # Parse tables from the Wikipedia web-page.
        wiki_tables = pd.read_html(self._wiki_url)
        # Get required tables from the list.
        current_state, historical_changes = self._get_required_wiki_tables(
            wiki_tables
        )
        # Save both DataFrames to csv files in the default directory.
        current_path, historical_path = self.get_file_paths(raw=True)
        _LOG.info("Writing current raw data to '%s'", current_path)
        current_state.to_csv(path_or_buf=current_path, index=False)
        _LOG.info("Writing historical raw data to '%s'", historical_path)
        historical_changes.to_csv(path_or_buf=historical_path, index=False)
        # Modify tables.
        current_state = self._parse_current_state(current_state)
        historical_changes = self._parse_historical_changes(historical_changes)
        # Save both DataFrames to csv files in the default directory.
        current_path, historical_path = self.get_file_paths()
        _LOG.info("Writing current data to '%s'", current_path)
        current_state.to_csv(path_or_buf=current_path, index=False)
        historical_changes.to_csv(path_or_buf=historical_path, index=False)
        _LOG.info("Writing historical data to '%s'", historical_path)

    # pylint: disable=no-self-use
    def _get_required_wiki_tables(
        self, wiki_tables: List[pd.DataFrame]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Identify current state table and historical changes table from the list and return them.

        :param wiki_tables: list of Wiki tables from the Wiki page
        :return: tuple of current state and historical changes tables
        """
        # The 1st table contains current state of the list.
        # The 2nd table contains historical changes to the list.
        return wiki_tables[0], wiki_tables[1]

    @staticmethod
    def _parse_historical_changes(
        historical_changes: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Parse and modify a Wiki table of historical changes of an SP* list.

        :param historical_changes: Wiki table as a pandas DataFrame
        :return: modified Wiki table
        """
        # Flatten MultiIndex columns.
        # Parsed columns from the Wiki by default:
        # date | added  | removed
        # date | ticker | ticker
        # Two column names rows above are concatenated with space:
        # date date | added ticker | removed ticker
        historical_changes.columns = [
            " ".join(col).strip().lower()
            for col in historical_changes.columns.values
        ]
        # Convert MultiIndex to Index.
        # The top level remains empty after concatenation.
        # Select the bottom level as column names, e.g. added ticker.
        historical_changes.columns = historical_changes.columns.get_level_values(
            0
        )
        # In case two levels have the same column name, change duplicated column names.
        historical_changes = historical_changes.rename(
            columns={"date date": "date", "reason reason": "reason"}
        )
        # Remove citation (e.g. [8]) from values in the date column.
        historical_changes["date"] = historical_changes["date"].str.replace(
            r"\[\d+\]", "", regex=True
        )
        # Convert date column from string to datetime format.
        historical_changes["date"] = pd.to_datetime(
            historical_changes["date"], infer_datetime_format=True
        )
        return historical_changes

    @staticmethod
    def _parse_current_state(current_state: pd.DataFrame) -> pd.DataFrame:
        """
        Parse and modify a Wiki table of current state of an SP* list.

        :param current_state: Wiki table as a pandas DataFrame
        :return: modified Wiki table
        """
        # Change tickers to map CIKs by edgar.map_identifiers.TickerCikMapper().
        current_state.loc[current_state["Symbol"] == "BF.B", "Symbol"] = "BFB"
        current_state.loc[current_state["Symbol"] == "BRK.B", "Symbol"] = "BRK-B"
        current_state.loc[current_state["Symbol"] == "GOOGL", "Symbol"] = "GOOG"
        return current_state


class SP600_Downloader(_SP_Downloader):
    def __init__(self) -> None:
        # Initialise parent class with hardcoded values.
        prefix = "sp600"
        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
        super().__init__(prefix, wiki_url)

    def _get_required_wiki_tables(
        self, wiki_tables: List[pd.DataFrame]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # The 4th table contains current state of the list.
        # There is no table that contains historical changes, so we emulate an empty one.
        return (
            wiki_tables[4],
            pd.DataFrame(
                columns=[
                    "date",
                    "added ticker",
                    "added security",
                    "removed ticker",
                    "removed security",
                    "reason",
                ]
            ),
        )

    @staticmethod
    def _parse_historical_changes(historical_changes: pd.DataFrame) -> pd.DataFrame:
        # No modifications to the empty table.
        return historical_changes

    @staticmethod
    def _parse_current_state(current_state: pd.DataFrame) -> pd.DataFrame:
        # Rename columns for SP600.
        current_state = current_state.rename(
            columns={
                "Company": "Security",
                "Ticker symbol": "Symbol",
                "GICS economic sector": "GICS Sector",
                "GICS sub-industry": "GICS Sub Industry",
            }
        )
        return current_state


class SP500_Downloader(_SP_Downloader):
    def __init__(self) -> None:
        # Initialise parent class with hardcoded values.
        prefix = "sp500"
        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        super().__init__(prefix, wiki_url)


class SP400_Downloader(_SP_Downloader):
    def __init__(self) -> None:
        # Initialise parent class with hardcoded values.
        prefix = "sp400"
        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
        super().__init__(prefix, wiki_url)

    @staticmethod
    def _parse_current_state(current_state: pd.DataFrame) -> pd.DataFrame:
        """
        Override parent method to preprocess columns from the Wiki table.

        :param current_state: Wiki table as a pandas DataFrame
        :return: modified Wiki table
        """
        # Rename columns for SP400.
        current_state = current_state.rename(
            columns={
                "Ticker symbol": "Symbol",
                "GICS sector": "GICS Sector",
                "GICS sub-industry": "GICS Sub Industry",
            }
        )
        # Process parent changes.
        current_state = super()._parse_current_state(current_state)
        return current_state


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Download SP400 constituents list and its historical changes.
    sp400 = SP400_Downloader()
    sp400.download_raw_data()
    # Download SP500 constituents list and its historical changes.
    sp500 = SP500_Downloader()
    sp500.download_raw_data()
    # Download SP600 constituents list and its historical changes.
    sp600 = SP600_Downloader()
    sp600.download_raw_data()


if __name__ == "__main__":
    _main(_parse())
