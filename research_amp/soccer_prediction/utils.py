"""
Import as:

import research_amp.soccer_prediction.utils as rasoprut
"""

import io
import logging
import os
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn.metrics as skm
import sklearn.model_selection as sms

import helpers.haws as haws
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def download_data_from_s3(
    bucket_name: str,
    dataset_path: str,
    local_path: str,
    *,
    file_format: str = ".txt",
    logging_level: int = logging.INFO,
) -> None:
    """
    Download files from S3.

    :param bucket_name: S3 bucket name.
    :param dataset_path: Path for the dataset in the S3 bucket.
    :param local_path: Destination path for downloading the dataset from
        the S3 to local machine.
    """
    # Initialize S3 session.
    s3 = haws.get_service_resource(aws_profile="ck", service_name="s3")
    # Fetch S3 bucket.
    bucket = s3.Bucket(bucket_name)
    # Define the local directory to save the files.
    os.makedirs(local_path, exist_ok=True)
    # Download the files the S3 path recursively.
    bucket = s3.Bucket(bucket_name)
    objects = list(bucket.objects.filter(Prefix=dataset_path))
    # Check for Null objects.
    if not objects:
        msg = "No files present in the S3 Location: "
        s3_path = f"s3://{bucket}/{dataset_path}"
        hdbg.dassert_eq(0, len(objects), msg, s3_path)
    for obj in bucket.objects.filter(Prefix=dataset_path):
        key = obj.key
        # Select the files that end with specified format.
        if key.endswith(file_format):
            local_file_path = os.path.join(local_path, os.path.basename(key))
            _LOG.log(logging_level, "Downloading %s to %s", key, local_file_path)
            bucket.download_file(key, local_file_path)
    _LOG.log(logging_level, "Data Downloaded.")


def load_data_to_dataframe(
    local_path: str,
    *,
    file_format: str = ".txt",
    logging_level: int = logging.INFO,
    sep: str = "\t",
    encoding: str = "UTF-8",
    **kwargs: Any,
) -> Dict:
    """
    Load datasets into pandas dataframe.

    :param local_path: Local directory where the S3 data was downloaded.
    :param file_format: The format of the files to be loaded. Default is
        ".txt".
    :param logging_level: Logging level. Default is logging.INFO.
    :param kwargs: Additional arguments to pass to pandas read_csv.
    :return: Dictionary of the datasets downloaded.
    """
    dataframes = {}
    # Iterate in the directory to collect the files and load them into dataframes.
    for dirname, _, filenames in os.walk(local_path):
        for filename in filenames:
            if filename.endswith(file_format):
                file_key = filename.split(".")[0] + "_df"
                filepath = os.path.join(dirname, filename)
                _LOG.log(logging_level, "Loading %s", filepath)
                df = pd.read_csv(filepath, sep=sep, encoding=encoding, **kwargs)
                _LOG.log(logging_level, " %s,  %s", file_key, df.shape)
                # Check if the dataframe is empty.
                if df.empty:
                    hdbg.dassert_eq(0, df.shape[0], "Empty Dataframe: ", file_key)
                # Drop duplicates.
                df = df.drop_duplicates()
                # Append to dictionary.
                dataframes[file_key] = df
    _LOG.log(logging_level, "Data loaded into dataframes.")
    # Return the dictionary of the dataframes.
    return dataframes


def save_data_to_s3(
    df: pd.DataFrame,
    bucket_name: str,
    s3_path: str,
    file_name: str,
    *,
    index: bool = False,
) -> None:
    """
    Save the give dataframe in S3  location as a `.csv` file.

    :param:
    """
    # Check if given dataframe is empty.
    if df.empty:
        raise ValueError("The given dataframe is empty.")
    # Save the dataframe a csv file in local directory.
    file_path = file_name + ".csv"
    df.to_csv(file_path, index=index)
    # Initiate S3 session.
    s3 = haws.get_service_resource(aws_profile="ck", service_name="s3")
    # Upload the file to S3.
    save_path = s3_path + "/" + file_path
    s3.Bucket(bucket_name).upload_file(file_path, save_path)


def compute_stats(df: pd.DataFrame, *, column: str = "WDL") -> Dict:
    """
    Compute # instances for each class (win, loss, draw) and prior
    probabilities.

    :param df: input DataFrame.
    :param column: column name in the DataFrame representing the match
        outcomes.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Check if the dataframe is empty
    hdbg.dassert_lt(0, df.shape[0])
    # Check if the column exists
    hdbg.dassert_in(column, df.columns)
    # Calculate value counts for each outcome.
    value_counts = df[column].value_counts()
    _LOG.info("Value Counts:\n%s", value_counts.to_string())
    # Calculate total number of matches.
    total_matches = len(df)
    # Calculate prior probabilities for each outcome.
    prior_probabilities = value_counts / total_matches
    _LOG.debug("\nPrior Probabilities:\n%s", prior_probabilities.to_string())
    stats = {"value_counts": value_counts, "priors": prior_probabilities}
    # Return stats
    return stats


def plot_metrics(
    train_accuracies: List,
    val_accuracies: List,
    *,
    feature_importances: pd.Series = None,
    coeffs: pd.Series = None,
    residuals: pd.Series = None,
    n_splits: int = 5,
) -> None:
    """
    Generalized function to plot various metrics for model evaluation.

    :param train_accuracies: List of training accuracies for each fold.
    :param val_accuracies: List of validation accuracies for each fold.
    :param feature_importances: Series of feature importances.
    :param coeffs: Series of model coefficients.
    :param residuals: Series of model residuals.
    :param n_splits: Number of cross-validation splits.
    :param feature_names: List of feature names for plotting feature
        importances.
    """
    # Plot training and validation accuracies over iterations.
    if train_accuracies is not None and val_accuracies is not None:
        plt.figure(figsize=(10, 6))
        plt.plot(
            range(1, n_splits + 1), train_accuracies, label="Training Accuracy"
        )
        plt.plot(
            range(1, n_splits + 1), val_accuracies, label="Validation Accuracy"
        )
        plt.xlabel("Fold")
        plt.ylabel("Accuracy")
        plt.title("Training and Validation Accuracy over Folds")
        plt.legend()
        plt.show()
    # Plot the feature importances.
    if feature_importances is not None:
        plt.figure(figsize=(10, 6))
        feature_importances.plot(kind="bar")
        plt.title("Feature Importances")
        plt.xlabel("Features")
        plt.ylabel("Importance Value")
        plt.show()
    # Plot the coefficients.
    if coeffs is not None:
        plt.figure(figsize=(10, 6))
        coeffs.plot(kind="bar")
        plt.title("Model Coefficients")
        plt.xlabel("Features")
        plt.ylabel("Coefficient Value")
        plt.show()
    # Plot the residuals.
    if residuals is not None:
        plt.figure(figsize=(10, 6))
        plt.hist(residuals, bins=30, edgecolor="k", alpha=0.7)
        plt.title("Distribution of Residuals")
        plt.xlabel("Residuals")
        plt.ylabel("Frequency")
        plt.show()


def recombine_predictions(test_df: pd.DataFrame) -> pd.DataFrame:
    """
    Recombine the test dataset with predictions into a single DataFrame.

    :param test_df: test dataset with home and away predictions.
    :return: recombined DataFrame with predictions.
    """
    hdbg.dassert_isinstance(test_df, pd.DataFrame)
    # Split the dataframe into home and away rows.
    home_df = test_df[test_df["is_home"] == 1].copy()
    away_df = test_df[test_df["is_home"] == 0].copy()
    # Rename columns for merging.
    home_df = home_df.rename(
        columns={
            "team": "HT",
            "opponent": "AT",
            "goals": "HS",
            "predicted_goals": "Lambda_HS",
        }
    )
    away_df = away_df.rename(
        columns={
            "team": "AT",
            "opponent": "HT",
            "goals": "AS",
            "predicted_goals": "Lambda_AS",
        }
    )
    # Merge the home and away dataframes.
    merged_df = pd.merge(
        home_df,
        away_df,
        on=["Date", "Sea", "Lge", "HT", "AT"],
        suffixes=("_home", "_away"),
    )
    # Select and reorder columns for the final dataframe.
    final_df = merged_df[
        ["Date", "Sea", "Lge", "HT", "AT", "HS", "AS", "Lambda_HS", "Lambda_AS"]
    ]
    # Return the final dataframe.
    return final_df


def evaluate_model_predictions(
    true_values: pd.Series, predicted_values: pd.Series
) -> None:
    """
    Evaluate the performance of the model.

    :true_values: true outcomes of the events. :predicted_values:
    outcomes predicted by the model
    """
    hdbg.dassert_isinstance(true_values, pd.Series)
    hdbg.dassert_isinstance(predicted_values, pd.Series)
    # Calculate accuracy.
    accuracy = skm.accuracy_score(true_values, predicted_values)
    print("Model Accuracy on Test Set:", accuracy)
    # Count each type of class prediction.
    predicted_counts = predicted_values.value_counts()
    actual_counts = true_values.value_counts()
    _LOG.info("Counts of each type of class prediction: \n%s", predicted_counts)
    _LOG.info("Counts of each type of actual outcomes: \n%s", actual_counts)


def representative_sample(
    df: pd.DataFrame, *, sample_size: int, column: str = "team"
) -> pd.DataFrame:
    """
    Perform representative sampling on training set to ensure each team is
    represented.

    :param df: input dataframe for sampling.
    :param sample_size: size of the extracted sample (output dataframe).
    :param column: column on which the sampling is performed.
    :return: sampled dataframe.
    """
    # Collect the unique values of teams.
    teams = df[column].unique()
    # Identify samples/team.
    samples_per_team = sample_size // len(teams)
    sampled_df_list = []
    # Iteratively add the samples for each team.
    for team in teams:
        team_df = df[df[column] == team]
        team_sample = team_df.sample(
            n=min(samples_per_team, len(team_df)), random_state=1
        )
        sampled_df_list.append(team_sample)
    # Create a sampled dataframe.
    sampled_df = pd.concat(sampled_df_list)
    # Additional random sampling to fill the remaining sample size.
    remaining_sample_size = sample_size - len(sampled_df)
    if remaining_sample_size > 0:
        additional_sample = df.drop(sampled_df.index).sample(
            n=remaining_sample_size, random_state=1
        )
        sampled_df = pd.concat([sampled_df, additional_sample])
    # Return the sampled dataframe.
    return sampled_df


def plot_learning_curves(
    training_costs: List[float],
    val_costs: List[float],
    sample_sizes: int,
) -> None:
    """
    Plot the learning curves of model.

    :param training_cost: cost function (MAE/MSE) values of training
        set.
    :param val_cost: cost function (MSE/MAE) values of validation set.
    """
    # Plot the learning curves
    plt.figure(figsize=(10, 6))
    plt.plot(sample_sizes, training_costs, label="Training MAE")
    plt.plot(sample_sizes, val_costs, label="Validation MAE")
    plt.xlabel("Sample Size")
    plt.ylabel("Cost function")
    plt.title("Learning Curves for Poisson Regression")
    plt.legend()
    plt.grid(True)
    plt.show()


def create_train_test_split(
    df: pd.DataFrame,
    *,
    stratify: bool = True,
    stratify_column: str = "HT",
    test_size: float = 0.2,
) -> Dict:
    """
    Create a train-test split with the preprocessed DataFrame.

    :param df: Input DataFrame.
    :param stratify: Whether to stratify the split based on the
        stratify_column. Default is True.
    :param stratify_column: The column to use for stratification if
        stratify is True. Default is "HT".
    :param test_size: Proportion of the dataset to include in the test
        split. Default is 0.2.
    :return: Dictionary of training and testing DataFrames.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Check if the dataframe is empty
    hdbg.dassert_lt(0, len(df))
    # Check if the column exists
    hdbg.dassert_in(stratify_column, df.columns)
    # Ensure reproducibility.
    random_state = 42
    # Split the DataFrame using stratify if specified.
    if stratify:
        train_df, test_df = sms.train_test_split(
            df,
            test_size=test_size,
            random_state=random_state,
            stratify=df[stratify_column],
        )
    else:
        train_df, test_df = sms.train_test_split(
            df, test_size=test_size, random_state=random_state
        )
    # Return the dictionary of DataFrames.
    dataframes = {"train_df": train_df, "test_df": test_df}
    return dataframes


def calculate_rps(
    df: pd.DataFrame,
    prob_home_win_col: str = "prob_home_win",
    prob_draw_col: str = "prob_draw",
    prob_away_win_col: str = "prob_away_win",
    actual_outcome_col: str = "actual_outcome",
) -> float:
    """
    Calculate the Rank Probability Score (RPS) for three-way outcome
    predictions in a DataFrame.

    :param df: df containing the columns for predicted probabilities and
        actual outcomes
    :param prob_home_win_col: column name for the probability of home
        win
    :param prob_draw_col: column name for the probability of draw
    :param prob_away_win_col: column name for the probability of away
        win
    :param actual_outcome_col: column name for the actual outcome
    :return: RPS score for the entire model
    """
    # Initialize RPS sum.
    rps_sum = 0
    # Calculate the total number of matches.
    M = len(df)
    # Iterate over each row in the DataFrame.
    for index, row in df.iterrows():
        # Extract predicted probabilities
        probs = np.array([row[prob_home_win_col], row[prob_away_win_col]])
        # Calculate cumulative predicted probabilities.
        probs.cumsum()
        # Determine actual outcomes.
        if row[actual_outcome_col] == "home_win":
            actuals = np.array([1, 0])
        elif row[actual_outcome_col] == "away_win":
            actuals = np.array([0, 1])
        else:
            actuals = np.array([0, 0])
        # Calculate RPS for the current match.
        rps = (np.sum((probs - actuals) ** 2)) / 2
        # Add the RPS of the current match to the total RPS sum.
        rps_sum += rps
    # Average RPS over all matches.
    rps_avg = rps_sum / M
    _LOG.debug("RPS value for the model: %.4f", rps_avg)
    return rps_avg


def load_data_from_s3(
    bucket_name: str,
    dataset_path: str,
    *,
    aws_profile: str = "ck",
    decoding: str = "utf-8",
    logging_level: int = logging.INFO,
    read_method: str = "read_csv",
    seperator: str = "\t",
    **kwargs,
) -> pd.DataFrame:
    """
    Load a single file from S3 directly into memory.

    :param bucket_name: S3 bucket
    :param dataset_path: path for the dataset in the S3 bucket
    :param aws_profile: associated profile for checking credentials.
    :param decoding: decoding type for the file content
    :param logging_level: logging level
    :param read_method: method used to read the file into df.(read_csv,
        read_excel, etc.)
    :return: DataFrame with the content of the file.
    """
    # Initialize S3 session.
    s3 = haws.get_service_resource(aws_profile=aws_profile, service_name="s3")
    # Retrieve the file from S3.
    obj = s3.Object(bucket_name, dataset_path)
    _LOG.log(
        logging_level, "Loading %s from bucket %s", dataset_path, bucket_name
    )
    data = obj.get()["Body"].read().decode(decoding)
    # Load data using the read method.
    read_func = getattr(pd, read_method)
    dataframe = read_func(io.StringIO(data), sep=seperator, **kwargs)
    _LOG.log(logging_level, "Data Loaded into Memory.")
    return dataframe


def poisson_probability(mean: float, k: int) -> float:
    """
    Calculate the Poisson probability.

    :param mean: the average rate (lambda) of the Poisson distribution
    :param k: the number of occurrences (k)
    :return: the Poisson probability of k occurrences
    """
    return (np.exp(-mean) * mean**k) / np.math.factorial(k)


# TODO(Krishna): Remove the unused/repeated functions.
def calculate_match_outcomes(
    df: pd.DataFrame, params: np.ndarray, *, max_goals: int = 10
) -> pd.DataFrame:
    """
    Calculate match outcome probabilities.

    :param df: input dataframe containing match data
    :param params: model parameters including team strengths and other
        factors
    :param max_goals: maximum number of goals to consider in the
        probability calculation
    :return: dataframe with added columns for the probabilities of home
        win, away win, and draw as well as the predicted outcomes
    """
    c, h, rho, *strengths = params
    # Calculate Lambda values for home and away teams.
    df["Lambda_HS"] = np.exp(
        c + df["HT_id"].apply(lambda x: strengths[int(x)]) + h
    )
    df["Lambda_AS"] = np.exp(
        c + df["AT_id"].apply(lambda x: strengths[int(x)]) - h
    )
    # Calculate probabilities of goals for home and away teams.
    home_goals_probs = np.array(
        [poisson_probability(df["Lambda_HS"], i) for i in range(max_goals)]
    )
    away_goals_probs = np.array(
        [poisson_probability(df["Lambda_AS"], i) for i in range(max_goals)]
    )
    prob_home_win = np.zeros(len(df))
    prob_away_win = np.zeros(len(df))
    prob_draw = np.zeros(len(df))
    # Calculate probabilities of match outcomes.
    for i in range(max_goals):
        for j in range(max_goals):
            prob = home_goals_probs[i] * away_goals_probs[j]
            prob_home_win += np.where(i > j, prob, 0)
            prob_away_win += np.where(i < j, prob, 0)
            prob_draw += np.where(i == j, prob, 0)
    # Add calculated probabilities and outcomes to the dataframe.
    df["prob_home_win"] = prob_home_win
    df["prob_away_win"] = prob_away_win
    df["prob_draw"] = prob_draw
    df["predicted_outcome"] = np.where(
        df["prob_home_win"] > df["prob_away_win"],
        "home_win",
        np.where(df["prob_away_win"] > df["prob_home_win"], "away_win", "draw"),
    )
    df["actual_outcome"] = np.where(
        df["HS"] > df["AS"],
        "home_win",
        np.where(df["HS"] < df["AS"], "away_win", "draw"),
    )
    df["Lambda_HS"] = df["Lambda_HS"].round().astype(int)
    df["Lambda_AS"] = df["Lambda_AS"].round().astype(int)
    return df


def get_outcome_probability(
    df: pd.DataFrame,
    params: np.ndarray,
    *,
    max_goals: int = 10,
) -> pd.DataFrame:
    """
    Calculate match outcome probabilities.

    :param df: input dataframe containing match data
    :param params: model parameters including team strengths and other
        factors
    :param max_goals: maximum number of goals to consider in the
        probability calculation
    :return: dataframe with added columns for the probabilities of home
        win, away win, and draw as well as the predicted outcomes
    """
    c, h, rho, *strengths = params
    # Calculate the average strength, excluding strengths still set to 1.
    valid_strengths = [s for s in strengths if s != 1]
    avg_strength = np.mean(valid_strengths) if valid_strengths else 1
    # Calculate Lambda values for home and away teams.
    df["lambda_goals_by_home_team"] = np.exp(
        c
        + df["home_team_id"].apply(
            lambda x: strengths[int(x)]
            if int(x) < len(strengths) and strengths[int(x)] != 1
            else avg_strength
        )
        + h
    )
    df["lambda_goals_by_opponent"] = np.exp(
        c
        + df["opponent_id"].apply(
            lambda x: strengths[int(x)]
            if int(x) < len(strengths) and strengths[int(x)] != 1
            else avg_strength
        )
        - h
    )
    # Calculate probabilities of goals for home and away teams.
    home_goals_probs = np.array(
        [
            poisson_probability(df["lambda_goals_by_home_team"], i)
            for i in range(max_goals)
        ]
    )
    away_goals_probs = np.array(
        [
            poisson_probability(df["lambda_goals_by_opponent"], i)
            for i in range(max_goals)
        ]
    )
    prob_win = np.zeros(len(df))
    prob_loss = np.zeros(len(df))
    prob_draw = np.zeros(len(df))
    # Calculate probabilities of match outcomes.
    for i in range(max_goals):
        for j in range(max_goals):
            prob = home_goals_probs[i] * away_goals_probs[j]
            prob_win += np.where(i > j, prob, 0)
            prob_loss += np.where(i < j, prob, 0)
            prob_draw += np.where(i == j, prob, 0)
    # Add calculated probabilities and outcomes to the dataframe.
    df["prob_win"] = prob_win
    df["prob_loss"] = prob_loss
    df["prob_draw"] = prob_draw
    df["lambda_goals_by_home_team"] = (
        df["lambda_goals_by_home_team"].round().astype(int)
    )
    df["lambda_goals_by_opponent"] = (
        df["lambda_goals_by_opponent"].round().astype(int)
    )
    return df[["prob_win", "prob_loss", "prob_draw"]]
