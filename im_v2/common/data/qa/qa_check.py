"""
Single dataset OHLCV QA checks.

Import as:

import im_v2.common.data.qa.qa_check as imvcdqqach
"""
from typing import Dict, List, Optional

import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import im_v2.common.data.transform.transform_utils as imvcdttrut
import sorrentum_sandbox.common.validate as ssacoval


def build_dummy_data_reconciliation_config() -> cconfig.ConfigList:
    """
    Dummy function to pass into amp/dev_scripts/notebooks/run_notebook.py as a
    configu_builder parameter.
    """
    config = cconfig.Config.from_dict({"dummy": "value"})
    config_list = cconfig.ConfigList([config])
    return config_list


def get_multilevel_bid_ask_column_names(*, depth: int = 10) -> List[str]:
    """
    Construct list of bid ask column names for multilevel setup.

    Example for depth = 2
        [bid_price_l1, bid_size_l1, ask_size_l1, ask_price_l1, bid_price_l2,...]
    """
    multilevel_bid_ask_cols = []
    for i in range(1, depth + 1):
        bid_ask_cols_level = map(lambda x: f"{x}_l{i}", imvcdttrut.BID_ASK_COLS)
        for col in bid_ask_cols_level:
            multilevel_bid_ask_cols.append(col)
    return multilevel_bid_ask_cols


class GapsInTimeIntervalCheck(ssacoval.QaCheck):
    """
    Check that all timestamps for given datasets are present.
    """

    def __init__(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        data_frequency: str,
    ) -> None:
        """
        :param start_timestamp: start datetime to check
        :param end_timestamp: end datetime to check
        :param data_frequency: "S" for second, "T" for minute.
        """
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.data_frequency = data_frequency

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        The method assumes presence of 'timestamp' (in UNIX format or
        pd.Timestamp) column, upon which the assertion is performed.

        :param datasets: list of pandas dataframes to check
        :return: result of checking
        """
        for data in datasets:
            current_gaps = hpandas.find_gaps_in_time_series(
                data["timestamp"],
                self.start_timestamp,
                self.end_timestamp,
                self.data_frequency,
            )
            if not current_gaps.empty:
                self._status = (
                    f"FAILED: Found gaps {current_gaps} in the dataset."
                )
                return False
        self._status = "PASSED"
        return True


class GapsInTimeIntervalBySymbolsCheck(ssacoval.QaCheck):
    """
    Check that all timestamps for given datasets grouped by
    currency_pair(symbols) are present.
    """

    def __init__(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        data_frequency: str,
    ) -> None:
        """
        :param start_timestamp: start datetime to check
        :param end_timestamp: end datetime to check
        :param data_frequency: "S" - second, "T" for minute.
        """
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.data_frequency = data_frequency

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        The method assumes presence of:

            - 'timestamp' (in UNIX format or pd.Timestamp) column, upon which the assertion is
            performed
            - 'currency_pair' column, that allow to iterate through

        :param datasets: list of pandas dataframes to check
        :return: result of checking
        """
        for data in datasets:
            if data.empty:
                self._status = "FAILED: The dataset is empty."
                return False
            for currency_pair in data["currency_pair"].unique():
                current_data = data[data["currency_pair"] == currency_pair]
                gaps_check = GapsInTimeIntervalCheck(
                    start_timestamp=self.start_timestamp,
                    end_timestamp=self.end_timestamp,
                    data_frequency=self.data_frequency,
                )
                if not gaps_check.check(datasets=[current_data]):
                    self._status = (
                        f"{gaps_check.get_status()}. "
                        f"Currency pair = {currency_pair}."
                    )
                    return False
        self._status = "PASSED"
        return True


class NaNChecks(ssacoval.QaCheck):
    """
    Check that datasets don't include NaN values.
    """

    def __init__(self, *, fields: Optional[List[str]] = None) -> None:
        self.fields = fields

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        :param datasets: list of pandas dataframes to check
        :param fields: list of fields to check,
            if not specified will check whole dataset
        :return: result of checking
        """
        for dataset in datasets:
            dataset_to_check = dataset[self.fields] if self.fields else dataset
            if dataset_to_check.isnull().any().any():
                self._status = (
                    f"FAILED: Found nulls values "
                    f"{dataset_to_check[dataset_to_check.isna().any(axis=1)]}\n"
                    "in the dataset."
                )
                return False
        self._status = "PASSED"
        return True


class OhlcvLogicalValuesCheck(ssacoval.QaCheck):
    """
    Execute the following checks:

    - volume is not 0
    - high >= low
    - high >= open and high >= close
    - low <= open  and low <= close
    """

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        :param datasets: list of pandas dataframes to check
        :return: result of checking
        """
        for data in datasets:
            check_result = self._check_dataset(data)
            failed_checks = [
                check_name
                for check_name, result in check_result.items()
                if not result
            ]
            if len(failed_checks) > 0:
                self._status = (
                    f"FAILED: next logical checks is not passed: "
                    f"{failed_checks}"
                )
                return False
        self._status = "PASSED"
        return True

    def _check_dataset(self, data: pd.DataFrame) -> Dict[str, bool]:
        """
        Check single dataset.

        :param dataset: pandas dataframe to check
        :return: list of the checks results like:
          {"name_of_the_check1": <boolean check result>}
        """
        return {
            # TODO(Juraj): temporarily disable this check since it can seldom
            #  happen that a given minute has 0 volume but it does not necessarily
            #  mean the data is malformed.
            # "volume_is_not_0": (data["volume"].ne(0)).all(),
            "high_gte_low": (data["high"] >= data["low"]).all(),
            "high_gte_open": (data["high"] >= data["open"]).all(),
            "high_gte_close": (data["high"] >= data["close"]).all(),
            "low_lte_open": (data["low"] <= data["open"]).all(),
            "low_lte_close": (data["low"] <= data["close"]).all(),
        }


class FullUniversePresentCheck(ssacoval.QaCheck):
    """
    Check that each currency pair (symbol) from a provided universe is present
    in the dataset.
    """

    def __init__(self, universe: List[str]) -> None:
        """

        :param universe: List of currency pair to check dataset(s) against.
        """
        self.universe = set(universe)

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        The method assumes presence of:

            - 'currency_pair' column, that allow to iterate through

        :param datasets: list of pandas dataframes to check
        :return: result of checking
        """
        for dataset in datasets:
            universe_set_diff = self.universe - set(
                dataset["currency_pair"].unique()
            )
            if universe_set_diff:
                self._status = f"FAILED: Found missing symbols in dataset:\n\t{universe_set_diff}"
                return False
        self._status = "PASSED"
        return True


class IdenticalDataFramesCheck(ssacoval.QaCheck):
    """
    Check that two DataFrames are are identical.
    """

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        The method assumes two datasets in pd.DataFrame format.

        :param datasets: list of pandas dataframes to check
        :return: True if DataFrames contain no differing rows, False otherwise
        """
        hdbg.dassert_eq(len(datasets), 2)
        # Compare dataframe contents.
        dataset_difference = hpandas.compare_dataframe_rows(
            datasets[0], datasets[1]
        )
        if not dataset_difference.empty:
            difference_signature = hpandas.get_df_signature(
                dataset_difference, num_rows=len(dataset_difference)
            )
            self._status = (
                f"FAILED: Differing table contents:\n\t{difference_signature}"
            )
            return False
        self._status = "PASSED"
        return True


class BidAskDataFramesSimilarityCheck(ssacoval.QaCheck):
    """
    Check that two DataFrames containing bid/ask contain 'almost the same
    values' based on specified accuracy threshold.

    The check currently assumes 10 levels of bid ask data.
    """

    def __init__(self, accuracy_threshold_dict: Dict[str, int]) -> None:
        """
        Constructor.

        :param accuracy_threshold_dict: dict in a format: column : threshold, where
         column is one of bid/ask data column from level 1 up to level 10, e.g. bid_price_l1.
         Threshold is a float between 0 and 1.
        """
        self.accuracy_threshold_dict = accuracy_threshold_dict

    def check(self, datasets: List[pd.DataFrame]) -> bool:
        """
        Perform an analysis based on relative differences between column. If
        the relative difference is higher than a desired threshold, the checks
        fails.

        :param datasets: list of pandas dataframes to check
        :return: analysis result
        """
        data = self._preprocess_datasets(datasets)
        bid_ask_cols = get_multilevel_bid_ask_column_names()
        # Each bid ask value will have a notional and a relative difference between two sources.
        for col in bid_ask_cols:
            # Notional difference: Dataset1 - Dataset2.
            data[f"{col}_diff"] = data[f"{col}_cc"] - data[f"{col}_ccxt"]
            # Relative value: (Dataset1 - Dataset2)/Dataset1.
            data[f"{col}_relative_diff_pct"] = (
                100
                * (data[f"{col}_cc"] - data[f"{col}_ccxt"])
                / data[f"{col}_ccxt"]
            )
        # Calculate the mean value of differences for each coin.
        diff_stats = []
        grouper = data.groupby(["currency_pair"])
        for col in bid_ask_cols:
            diff_stats.append(grouper[f"{col}_diff"].mean())
            diff_stats.append(grouper[f"{col}_relative_diff_pct"].mean())
        diff_stats = pd.concat(diff_stats, axis=1)
        error_message = []
        # Log the difference.
        for index, row in diff_stats.iterrows():
            for col in get_multilevel_bid_ask_column_names():
                cur_threshold = self.accuracy_threshold_dict[col]
                cur_col = f"{col}_relative_diff_pct"
                if abs(row[cur_col]) > cur_threshold:
                    message = (
                        f"Difference in {col}"
                        f" for `{index}` coin is {abs(row[cur_col])}% (> {cur_threshold}% threshold)."
                    )
                    error_message.append(message)
        if error_message:
            error_message = "\n".join(error_message)
            self._status = f"FAILED :\n\t{error_message}"
        else:
            self._status = "PASSED"
        return self._status == "PASSED"

    def _preprocess_datasets(self, datasets: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Merge two datasets in order to compute column-wise comparison of
        values.
        """
        datasets = list(
            map(
                lambda data: data.set_index(
                    ["timestamp", "currency_pair"], drop=True
                ),
                datasets,
            )
        )
        # TODO(Juraj): for now we perform comparison on the intersection
        #  of both universes
        data = datasets[0].merge(
            datasets[1],
            left_index=True,
            right_index=True,
            # TODO(Juraj) handle the suffixes better.
            suffixes=("_ccxt", "_cc"),
        )
        # Move the same metrics from two vendors together.
        data = data.reindex(sorted(data.columns), axis=1)
        return data
