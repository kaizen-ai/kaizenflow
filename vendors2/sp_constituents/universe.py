#!/usr/bin/env python
"""
Based on downloaded csv files of current SP* constituents list construct state as of a date.

Import as:
import vendors2.sp_constituents.universe as sp_uv
"""

import datetime
import functools
import logging
import os
from typing import Optional, Tuple

import pandas as pd

import helpers.git as git

_LOG = logging.getLogger(__name__)


class SP_Universe:
    def __init__(self, prefix: str, dir_name: Optional[str] = None) -> None:
        """
        Initialize the SP*_Universe by loading the files with the SP* constituents.

        :param dir_name: directory with the SP* data
        """
        self._prefix = prefix
        # Load current state, historical changes and manual changes from csv files.
        current_path, historical_path, manual_path = self.get_file_paths(dir_name)
        self.current_state = pd.read_csv(current_path)
        self.historical_changes = pd.read_csv(historical_path)
        self.manual_changes = pd.read_csv(manual_path, keep_default_na=False)
        # Get history of additions and deletions.
        self.additions = self._get_historical_additions()
        self.deletions = self._get_historical_deletions()
        # Get current date.
        self.start_date = datetime.date.today().strftime("%Y-%m-%d")

    def get_file_paths(
        self, dir_name: Optional[str] = None
    ) -> Tuple[str, str, str]:
        """
        Get full paths to the csv files with the constituents lists.

        :param dir_name: directory with the data
        :return: full paths to the current state of constituents and its historical changes
        """
        root_dir = git.get_client_root(True)
        prefix = self._prefix
        dir_name = dir_name if dir_name else "vendors2/sp_constituents/data"
        dst_dir = os.path.join(root_dir, dir_name)
        path_current = os.path.join(dst_dir, prefix + ".current_state.csv")
        path_changes = os.path.join(dst_dir, prefix + ".historical_changes.csv")
        path_manual = os.path.join(dst_dir, prefix + ".manual_changes.csv")
        return path_current, path_changes, path_manual

    def get_constituents(self, as_of_date: str) -> pd.DataFrame:
        """
        Get a list of tickers in SP* constituents list as of date.

        :param as_of_date: date of the list's state
        :return: SP* constituents list as of date
        """
        # Get list of tickers in the current SP* list.
        state_as_of_date = self.current_state["Symbol"].to_list()
        # Get dates back from current day to the user-specified date.
        date_range = pd.date_range(
            start=self.start_date, end=as_of_date, freq="-1D"
        )
        # Go back in history day by day.
        for curr_date in date_range:
            curr_date = curr_date.strftime("%Y-%m-%d")
            # Update SP* constituents list only in case of changes on that date.
            if any(self.deletions["date"] == curr_date) or any(
                self.additions["date"] == curr_date
            ):
                # List all changes that took place on the date.
                tickers_to_add = self.deletions.loc[
                    self.deletions["date"] == curr_date, "removed ticker"
                ].to_list()
                tickers_to_remove = self.additions.loc[
                    self.additions["date"] == curr_date, "added ticker"
                ].to_list()
                # Add back the deletions to the SP* constituents list.
                state_as_of_date = state_as_of_date + tickers_to_add
                # Reverse the additions to the SP* constituents list.
                state_as_of_date = [
                    x for x in state_as_of_date if x not in set(tickers_to_remove)
                ]
        # Sort and remove duplicates.
        state_as_of_date = list(set(state_as_of_date))
        state_as_of_date.sort()
        # Convert tickers list to a DataFrame.
        state_as_of_date = pd.DataFrame(state_as_of_date, columns=["ticker"])
        state_as_of_date["date"] = as_of_date
        return state_as_of_date

    def _get_historical_additions(self) -> pd.DataFrame:
        """
        Get a list of all historical additions to the SP* constituents list.

        :return: tickers added to the list and date of addition
        """
        # Keep only dates of changes and tickers' names.
        additions = self.historical_changes[["date", "added ticker"]].copy()
        additions = additions.dropna()
        # Modify additions with manual changes.
        for _, row in self.manual_changes.iterrows():
            if not row["added ticker"]:
                continue
            if row["added security"] == "drop":
                additions = additions.drop(
                    additions[
                        (additions["added ticker"] == row["added ticker"])
                        & (additions["date"] == row["date"])
                    ].index
                )
            else:
                additions = additions.append(
                    {"date": row["date"], "added ticker": row["added ticker"]},
                    ignore_index=True,
                )
        return additions

    def _get_historical_deletions(self) -> pd.DataFrame:
        """
        Get a list of all historical deletions from the SP* constituents list.

        :return: tickers deleted from the list and date of deletion
        """
        # Keep only dates of changes and tickers' names.
        deletions = self.historical_changes[["date", "removed ticker"]].copy()
        deletions = deletions.dropna()
        # Modify deletions with manual changes.
        for _, row in self.manual_changes.iterrows():
            if not row["removed ticker"]:
                continue
            if row["removed security"] == "drop":
                deletions = deletions.drop(
                    deletions[
                        (deletions["removed ticker"] == row["removed ticker"])
                        & (deletions["date"] == row["date"])
                    ].index
                )
            else:
                deletions = deletions.append(
                    {
                        "date": row["date"],
                        "removed ticker": row["removed ticker"],
                    },
                    ignore_index=True,
                )
        return deletions


@functools.lru_cache()
def get_sp600_universe() -> SP_Universe:
    """
    Create SP_Universe for SP600 list.

    :return: SP_Universe instance
    """
    return SP_Universe("sp600")


@functools.lru_cache()
def get_sp500_universe() -> SP_Universe:
    """
    Create SP_Universe for SP500 list.

    :return: SP_Universe instance
    """
    return SP_Universe("sp500")


@functools.lru_cache()
def get_sp400_universe() -> SP_Universe:
    """
    Create SP_Universe for SP500 list.

    :return: SP_Universe instance
    """
    return SP_Universe("sp400")
