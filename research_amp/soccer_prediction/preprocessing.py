"""
Import as:

import research_amp.soccer_prediction.preproccesing as rasoprpr
"""
import logging
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, LabelEncoder

import helpers.hdbg as hdbg
import research_amp.soccer_prediction.utils as rasoprut

_LOG = logging.getLogger(__name__)


class GeneralPreprocessing(BaseEstimator, TransformerMixin):
    """
    Perform general preprocessing on the dataframe

    - Convert 'Date' to datetime and sort
    - Change column names to a more readable format
    - Change match_outcome values to a more readable format
    - Filter by leagues and seasons if provided
    """

    def __init__(self, *, leagues: Optional[List[str]] = None, seasons: Optional[List[str]] = None):
        self.leagues = leagues
        self.seasons = seasons

    def fit(self,
    X: pd.DataFrame,
    ) -> None:
        """
        Fit method, which does nothing and is here for compatibility

        :param X: input DataFrame
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform method to apply general preprocessing

        :param X: input DataFrame
        :return: preprocessed DataFrame
        """
        # Validate input.
        hdbg.dassert_isinstance(X, pd.DataFrame)
        # Convert 'Date' to datetime and sort.
        rows_before = len(X)
        # Add 'season' column.
        X["season"] = X["Sea"].apply(lambda x: "20" + str(x)[:2])
        # Filter by leagues and seasons if provided.
        if self.leagues is not None:
            X = X[X["Lge"].isin(self.leagues)]
        if self.seasons is not None:
            X = X[X["season"].isin(self.seasons)]
        X.drop(columns = "season", inplace=True)
        X["Date"] = pd.to_datetime(X["Date"], dayfirst=True)
        X.sort_values(by="Date", inplace=True)
        # Rename columns in the dataframe.
        rename_columns = {
            "Sea": "season",
            "Lge": "league",
            "HT": "home_team",
            "AT": "opponent",
            "AS": "goals_scored_by_opponent",
            "HS": "goals_scored_by_home_team",
            "WDL": "match_outcome",
            "GD": "goal_difference",
        }
        X = X.rename(columns=rename_columns)
        # Change `match_outcome` values.
        new_values = {"W": "win", "L": "loss", "D": "draw"}
        X["match_outcome"] = X["match_outcome"].replace(new_values)
        # Convert categorical columns to 'category' type.
        categorical_columns = ["home_team", "opponent"]
        for col in categorical_columns:
            X[col] = X[col].astype("category")
        # Sort the dataframe by date.
        X = X.sort_values(by="Date").reset_index(drop=True)
        # Separate matches on the same day by 20 seconds.
        X["Time_Adjusted"] = X.groupby("Date").cumcount() * 20
        X["Adjusted_Date"] = X["Date"] + pd.to_timedelta(
            X["Time_Adjusted"], unit="s"
        )
        # Set the 'Adjusted_Date' as the index
        X = X.set_index("Adjusted_Date")
        X = X.drop(columns=["Time_Adjusted"])
        # Generate unique team identifiers.
        teams = pd.Series(
            X["home_team"].tolist() + X["opponent"].tolist()
        ).unique()
        team_to_id = {team: idx for idx, team in enumerate(teams)}
        # Map teams to unique identifiers.
        X["home_team_id"] = X["home_team"].map(team_to_id).astype(int)
        X["opponent_id"] = X["opponent"].map(team_to_id).astype(int)
        return X


class LabelEncoding(BaseEstimator, TransformerMixin):
    """
    Apply Label Encoding to specified categorical columns.

    :param columns: List of columns to encode.
    """

    def __init__(self, columns: List[str]):
        self.columns = columns
        self.label_encoders = {}

    def fit(
        self,
        X: pd.DataFrame,
    ) -> "LabelEncoding":
        """
        Fit method to learn the encoding for each column.

        :param X: input DataFrame.
        :param y: ignored.
        :return: self
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        hdbg.dassert_lt(0, X.shape[0])
        for col in self.columns:
            # Check if the column is present.
            hdbg.dassert_in(col, X.columns)
            # Encode the column values.
            le = LabelEncoder()
            le.fit(X[col])
            self.label_encoders[col] = le
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform method to apply label encoding.

        :param X: Input DataFrame.
        :return: DataFrame with encoded columns.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        hdbg.dassert_lt(0, X.shape[0])
        for col in self.columns:
            # Check if the column is present.
            hdbg.dassert_in(col, X.columns)
            # Apply label encoding.
            le = self.label_encoders[col]
            X[col] = le.transform(X[col])
        return X

    def inverse_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Inverse transform method to revert label encoding.

        :param X: Input DataFrame.
        :return: DataFrame with decoded columns.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        hdbg.dassert_lt(0, X.shape[0])
        X_inverse_transformed = X.copy()
        for col in self.columns:
            # Check if the column is present.
            assert (
                col in X.columns
            ), f"Column '{col}' not found in input DataFrame."
            # Apply the inverse label encoding.
            X_inverse_transformed[col] = self.label_encoders[
                col
            ].inverse_transform(X[col])
        return X_inverse_transformed


def add_epsilon(
    X: pd.DataFrame, *, epsilon: float = 0.5, columns: Optional[List[Any]] = None
) -> pd.DataFrame:
    """
    Add epsilon to specified columns to avoid log(0).

    :param X: Input DataFrame.
    :param epsilon: Value to add to columns. Default is 0.5.
    :param columns: List of columns to add epsilon to.
    :return: DataFrame with epsilon added to specified columns.
    """
    hdbg.dassert_isinstance(X, pd.DataFrame)
    hdbg.dassert_lt(0, X.shape[0])
    if columns is not None:
        for column in columns:
            hdbg.dassert_in(column, X.columns)
            # Add epsilon to specified columns.
            affected_rows_count = X[column].apply(lambda x: x == 0).sum()
            X[column] = X[column].apply(lambda x: x + epsilon if x == 0 else x)
            _LOG.info(
                "Column '%s' had %d rows affected.", column, affected_rows_count
            )
    else:
        raise ValueError("Columns parameter cannot be None.")
    return X


class UnravelData(BaseEstimator, TransformerMixin):
    """
    Unravel the dataset by creating two entries for each row as team-opponent
    pairs.
    """

    def fit(
        self,
        X: pd.DataFrame,
    ) -> "UnravelData":
        """
        Fit method, which does nothing and is here for compatibility.

        :param X: input DataFrame.
        :return: self
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform method to unravel data into team-opponent pairs.

        :param X: Input DataFrame.
        :return: Unraveled DataFrame.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        # Create entry for home team 'HT'.
        home_df = X[["Date", "Sea", "Lge", "HT", "AT", "HS"]].copy()
        home_df.rename(
            columns={"HT": "team", "AT": "opponent", "HS": "goals"}, inplace=True
        )
        home_df["is_home"] = 1
        # Create entry for away team 'AT'.
        away_df = X[["Date", "Sea", "Lge", "HT", "AT", "AS"]].copy()
        away_df.rename(
            columns={"AT": "team", "HT": "opponent", "AS": "goals"}, inplace=True
        )
        away_df["is_home"] = 0
        # Concatenate the two splits.
        unraveled_df = pd.concat([home_df, away_df], ignore_index=True)
        return unraveled_df


class Dropnan(BaseEstimator, TransformerMixin):
    """
    Drop rows with NaN and infinite values.
    """

    def fit(
        self,
        X: pd.DataFrame,
    ) -> "Dropnan":
        """
        Fit method, which does nothing and is here for compatibility.

        :param X: input DataFrame.
        :return: self
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform method to drop rows with NaN and infinite values.

        :param X: Input DataFrame.
        :return: DataFrame with rows containing NaN and infinite values
            dropped.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        # Check if the the dataframe is empty
        hdbg.dassert_lt(0, X.shape[0])
        # Drop rows with NaN values.
        X.dropna(inplace=True)
        # Drop rows with infinite values.
        X.replace([np.inf, -np.inf], np.nan, inplace=True)
        X.dropna(inplace=True)
        return X

def load_and_preprocess_data(
    *,
    bucket_name: str,
    dataset_path: str,
    logging_level: int = logging.INFO,
    add_epsilon: bool = False,
    epsilon_val: float = 0.5,
    add_epsilon_to_columns: Optional[List[str]] = None,
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load and preprocess df.

    :param bucket_name: S3 bucket name
    :param dataset_path: path to the dataset in the S3 bucket
    :param logging_level: logging level
    :param add_epsilon: flag to add epsilon value to columns
    :param epsilon_val: epsilon value to add
    :param add_epsilon_to_columns: list of columns to which epsilon should be added
    :param leagues: list of leagues to filter by
    :param seasons: list of seasons to filter by
    :return: processed DataFrame
    """
    # Load df.
    df = rasoprut.load_data_from_s3(bucket_name, dataset_path)
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Check if loaded df is empty.
    hdbg.dassert_ne(0, len(df), "Dataframe is empty.")
    # Create preprocessing pipeline.
    pipeline_steps = [
        ("general_preprocessing", GeneralPreprocessing(leagues=leagues, seasons=seasons)),
        ("drop_nan_and_inf", Dropnan()),
    ]
    # Add epsilon value to goals scored.
    if add_epsilon:
        pipeline_steps.append(
            (
                "add_epsilon",
                FunctionTransformer(
                    add_epsilon,
                    kw_args={
                        "epsilon": epsilon_val,
                        "columns": add_epsilon_to_columns,
                    },
                ),
            )
        )
    pipeline = Pipeline(pipeline_steps)
    # Apply the initial pipeline to the data.
    preprocessed_df = pipeline.fit_transform(df)
    return preprocessed_df
