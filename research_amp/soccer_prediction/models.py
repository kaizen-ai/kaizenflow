"""
Import as:

import research_amp.soccer_prediction.models as rasoprmo
"""
import collections
import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import scipy.optimize as spop
import sklearn.linear_model as slm
import statsmodels.api as sm
import statsmodels.formula.api as smf
from sklearn.base import BaseEstimator, RegressorMixin

import dataflow.core.node as dtfcornode
import dataflow.core.nodes.base as dtfconobas
import dataflow.core.utils as dtfcorutil
import helpers.hdbg as hdbg

# import xgboost as xgb
# import jpickle
import sklearn.metrics as skm
import sklearn.model_selection as sms

import helpers.hdbg as hdbg
import research_amp.soccer_prediction.utils as rasoprut

_LOG = logging.getLogger(__name__)


class BaseModel:
    """
    Define standard interface for models with fit and predict methods.
    """

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Fit the model using the training data.

        :param X: Feature matrix.
        :param y: Target vector.
        """
        raise NotImplementedError("Subclasses should implement this method.")

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict using the fitted model.

        :param X: Feature matrix.
        :return: Predictions as a NumPy array.
        """
        raise NotImplementedError("Subclasses should implement this method.")


class PoissonModel(BaseModel):
    """
    PoissonModel implements a Poisson regression model using statsmodels.
    """

    def __init__(self, *, hyperparams: Optional[Dict[str, Any]] = None):
        self.hyperparams = hyperparams or {}
        self.model = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Fit the Poisson regression model.

        :param X: Feature matrix as a DataFrame.
        :param y: Target vector as a Series.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        hdbg.dassert_isinstance(y, pd.Series)
        data = pd.concat([X, y], axis=1)
        formula = self.hyperparams.get("formula", "")
        max_iter = self.hyperparams.get("maxiter", 10)
        self.model = smf.glm(
            formula=formula, data=data, family=sm.families.Poisson()
        ).fit(maxiter=max_iter)
        _LOG.info("Poisson model fitted successfully.")
        _LOG.info(self.model.summary())

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict using the fitted Poisson regression model.

        :param X: Feature matrix as a DataFrame.
        :return: Predictions as a NumPy array.
        """
        if self.model is not None:
            y_pred = self.model.predict(X)
        else:
            raise ValueError("Model is not fitted yet.")
        return y_pred


# class XGBoostModel(BaseModel):
#    """
#    XGBoostModel implements a regression model using xgboost.XGBRegressor.
#    """
#
#   def __init__(self, hyperparams: Optional[Dict[str, Any]] = None):
#        self.hyperparams = hyperparams or {}
#        self.model = xgb.XGBRegressor(**self.hyperparams, random_state=42)

#    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
#        """
#        Fit the XGBoost regression model.

#        :param X: Feature matrix as a DataFrame.
#        :param y: Target vector as a Series.
#        """
#        hdbg.dassert_isinstance(X, pd.DataFrame)
#        hdbg.dassert_isinstance(y, pd.Series)
#        self.model.fit(X, y)
#        _LOG.info("XGBoost model fitted successfully.")
#        _LOG.info("Feature importances: %s", self.model.feature_importances_)

#    def predict(self, X: pd.DataFrame) -> np.ndarray:
#        """
#        Predict using the fitted XGBoost regression model.
#
#        :param X: Feature matrix as a DataFrame.
#        :return: Predictions as a NumPy array.
#        """
#        if self.model is not None:
#            y_pred = self.model.predict(X)
#        else:
#            raise ValueError("Model is not fitted yet.")
#        return y_pred


class LinearRegressionModel(BaseModel):
    """
    LinearRegressionModel implements a linear regression model using sklearn.
    """

    def __init__(self, *, hyperparams: Optional[Dict[str, Any]] = None):
        self.hyperparams = hyperparams or {}
        self.model = slm.LinearRegression(**self.hyperparams)

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Fit the linear regression model.

        :param X: Feature matrix as a DataFrame.
        :param y: Target vector as a Series.
        """
        hdbg.dassert_isinstance(X, pd.DataFrame)
        hdbg.dassert_isinstance(y, pd.Series)
        self.model.fit(X, y)
        _LOG.info("Linear regression model fitted successfully.")
        _LOG.info("Coefficients: %s", self.model.coef_)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict using the fitted linear regression model.

        :param X: Feature matrix as a DataFrame.
        :return: Predictions as a NumPy array.
        """
        if self.model is not None:
            y_pred = self.model.predict(X)
        else:
            raise ValueError("Model is not fitted yet.")
        return y_pred


def train_model(
    train_df: pd.DataFrame,
    model: BaseModel,
    hyperparameters: Dict,
    *,
    target_column: str = "goals",
    logging_level: int = logging.INFO,
    n_splits: int = 5,
    **kwargs: Any,
) -> BaseModel:
    """
    Train a model log the results, and save the trained model to S3.

    :param train_df: Input training set.
    :param model: Model object that implements fit() and predict().
    :param hyperparameters: dictionary of hyperparameters for models.
    :param target_columns: target column/variable of interest.
    :param logging_level: Logging level for the model summary.
    :param n_splits: Number of splits for cross-validation.
    :return: Trained model.
    """
    # Configure logging.
    logging.basicConfig(level=logging_level)
    train_accuracies = []
    val_accuracies = []
    train_mae = []
    val_mae = []
    model.hyperparams = hyperparameters
    sample_sizes = kwargs.get("sample_sizes", [20000, 30000, 40000, 50000])
    # Split the data into training and validation sets.
    for sample_size in sample_sizes:
        # Sample the training data.
        sample_train_data = train_df.sample(n=sample_size, random_state=42)
        train_data, val_data = sms.train_test_split(
            sample_train_data, test_size=0.2, random_state=42
        )
        y_train = train_data[target_column]
        X_train = train_data.drop(target_column, axis=1)
        y_val = val_data[target_column]
        X_val = val_data.drop(target_column, axis=1)
        # Fit the Poisson regression model on the sampled training data.
        model.fit(X_train, y_train)
        train_preds = model.predict(X_train)
        val_preds = model.predict(X_val)
        # Predict on training and validation sets.
        train_preds_rounded = np.round(train_preds).astype(int)
        val_preds_rounded = np.round(val_preds).astype(int)
        y_train = y_train.astype(int)
        y_val = y_val.astype(int)
        train_acc = skm.accuracy_score(y_train, train_preds_rounded)
        val_acc = skm.accuracy_score(y_val, val_preds_rounded)
        train_accuracies.append(train_acc)
        val_accuracies.append(val_acc)
        # Calculate MAE for training and validation sets.
        train_mae_value = skm.mean_absolute_error(y_train, train_preds)
        val_mae_value = skm.mean_absolute_error(y_val, val_preds)
        train_mae.append(train_mae_value)
        val_mae.append(val_mae_value)
        logging.info(
            "Sample Size: %f, Train MAE: %f, Validation MAE: %f",
            sample_size,
            train_mae_value,
            val_mae_value,
        )
    # Plot metrics.
    rasoprut.plot_learning_curves(train_mae, val_mae, sample_sizes)
    coeffs = model.model.params
    # Plot the residuals.
    residuals = model.model.resid_deviance
    rasoprut.plot_metrics(
        train_accuracies=train_accuracies,
        val_accuracies=val_accuracies,
        coeffs=coeffs,
        residuals=residuals,
        n_splits=len(sample_sizes),
    )
    max_index = np.argmax(val_accuracies)
    train_data = train_df.sample(n=sample_sizes[max_index], random_state=42)
    y_train = train_data[target_column]
    X_train = train_data.drop(target_column, axis=1)
    # Fit the Poisson regression model on the sampled training data
    model.fit(X_train, y_train)
    return model


def save_model_to_s3(
    model: BaseModel,
    local_path: str,
    s3_bucket: str,
    s3_path: str,
    aws_profile: str,
) -> None:
    """
    Save the model locally and upload to S3.

    :param model: Trained model to be saved.
    :param local_path: Local path to save the model.
    :param s3_bucket: S3 bucket name.
    :param s3_path: S3 path where the model will be uploaded.
    :param aws_profile: AWS profile name.
    """
    # Save the model locally.
    # jpickle.dump(model, local_path)
    # Upload the model to S3.
    # s3 = haws.get_service_resource(aws_profile=aws_profile, service_name="s3")
    # s3.Bucket(s3_bucket).upload_file(local_path, s3_path)


class BivariatePoissonWrapper(BaseEstimator, RegressorMixin):
    """
    A wrapper for the bivariate Poisson model to make it compatible with the
    scikit-learn interface.
    """

    def __init__(self, maxiter: int = 100):
        """
        Initialize the BivariatePoissonWrapper with the maximum iterations.

        :param maxiter: maximum number of iterations for fitting the
            model
        """
        self.maxiter = maxiter
        self.params = None
        self.data = None  # To store data used in fitting

    def bivariate_poisson_log_likelihood(
        self, params: np.ndarray, data: pd.DataFrame
    ) -> float:
        """
        Calculate the negative log likelihood for the bivariate Poisson model.

        :param params: model parameters including team strengths and other factors
                       the expected format is [c, h, rho, *strengths], where:
                       - c: constant term
                       - h: home advantage term
                       - rho: correlation term
                       - strengths: strengths of the teams
        :param data: df with the data
        :return: negative log likelihood
        """
        c, h, rho, *strengths = params
        log_likelihood = 0
        for _, row in data.iterrows():
            # Extract and convert necessary variables.
            i, j, goals_i, goals_j, time_weight = (
                int(row["HT_id"]),
                int(row["AT_id"]),
                int(row["HS"]),
                int(row["AS"]),
                row["Time_Weight"],
            )
            # Check for out-of-bounds indices.
            if i >= len(strengths) or j >= len(strengths):
                print(
                    f"Index out of bounds: i={i}, j={j}, len(strengths)={len(strengths)}"
                )
                continue
            # Calculate lambda values.
            lambda_i = np.exp(c + strengths[i] + h)
            lambda_j = np.exp(c + strengths[j] - h)
            joint_prob = 0
            # Compute joint probability.
            for k in range(min(goals_i, goals_j) + 1):
                P_goals_i = rasoprut.poisson_probability(lambda_i, goals_i)
                P_goals_j = rasoprut.poisson_probability(lambda_j, goals_j)
                joint_prob += P_goals_i * P_goals_j
            log_likelihood += time_weight * np.log(joint_prob)
        return -log_likelihood

    def fit(
        self,
        X: pd.DataFrame,
        y: Optional[pd.DataFrame] = None,
        sample_weight: Optional[np.ndarray] = None,
    ) -> None:
        """
        Fit the bivariate Poisson model to the given data.

        :param X: features (design matrix) for the regression
        :param y: Target variable (DataFrame or ndarray)
        :param sample_weight: Ignored, added for compatibility
        """
        if y is not None:
            # Convert numpy arrays to DataFrames if necessary.
            if isinstance(X, np.ndarray):
                X = pd.DataFrame(X, columns=["HT_id", "AT_id", "Time_Weight"])
            if isinstance(y, np.ndarray):
                y = pd.DataFrame(y, columns=["HS", "AS"])
            # Combine features and target into a single DataFrame.
            data = pd.concat(
                [X.reset_index(drop=True), y.reset_index(drop=True)], axis=1
            )
        else:
            data = X
        # Ensure HT_id and AT_id are integers.
        data["HT_id"] = data["HT_id"].astype(int)
        data["AT_id"] = data["AT_id"].astype(int)
        # Store the data for use in predict.
        self.data = data
        # Calculate the number of teams.
        num_teams = max(data["HT_id"].max(), data["AT_id"].max()) + 1
        # Initialize parameters for optimization.
        initial_params = [0, 0, 0.1] + [1] * num_teams
        # Set optimization options.
        options = {"maxiter": self.maxiter, "disp": False}
        # Minimize the negative log likelihood and capture the display output.
        _LOG.info("Starting optimization process...")
        result = spop.minimize(
            self.bivariate_poisson_log_likelihood,
            initial_params,
            args=(data,),
            method="L-BFGS-B",
            options=options,
            callback=lambda xk: _LOG.info(f"Current params: {xk}"),
        )
        # Store the optimized parameters.
        self.params = result.x
        return self

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Predict match outcomes using the fitted bivariate Poisson model.

        :param X: features (design matrix) for the prediction
        :return: predicted values
        """
        # Ensure that self.data is used in calculate_match_outcomes.
        df_out = rasoprut.calculate_match_outcomes(self.data, self.params)
        return df_out[["Lambda_HS", "Lambda_AS"]].values

    def score(
        self,
        X: pd.DataFrame,
        y: pd.DataFrame,
        sample_weight: Optional[np.ndarray] = None,
    ) -> float:
        """
        Score the model using the accuracy of the predicted outcomes.

        :param X: features (design matrix) for the scoring
        :param y: true target values
        :param sample_weight: ignored, added for compatibility
        :return: accuracy score
        """
        # Predict match outcomes.
        predictions = self.predict(X)
        # Calculate the number of correct predictions.
        correct_predictions = (
            predictions["predicted_outcome"] == predictions["actual_outcome"]
        ).sum()
        # Calculate and return accuracy.
        return correct_predictions / len(predictions)

    def get_fit_state(self) -> Dict[str, Any]:
        """
        Get the current fit state of the model.

        :return: dictionary containing the fit state of the model
        """
        return {"params": self.params}

    def set_fit_state(self, fit_state: Dict[str, Any]) -> None:
        """
        Set the fit state of the model.

        :param fit_state: dictionary containing the fit state of the
            model
        """
        self.params = fit_state["params"]


class BivariatePoissonModel(dtfconobas.FitPredictNode, dtfconobas.ColModeMixin):
    """
    Bivariate Poisson Model.
    """

    def __init__(
        self,
        nid: dtfcornode.NodeId,
        maxiter: int = None,
        col_mode: Optional[str] = None,
        half_life_period: Optional[int] = None,
    ):
        """
        Initialize the BivariatePoissonModel with the maximum iterations.

        :param nid: unique node id
        :param maxiter: maximum number of iterations for fitting the
            model
        :param col_mode: "merge_all" or "replace_all", as in
            `ColumnTransformer()`
        :param half_life_period: half-life period for time weighting
        """
        super().__init__(nid)
        self.maxiter = maxiter
        self.params = None
        self.data = None
        self._col_mode = col_mode or "replace_all"
        hdbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])
        self.half_life_period = half_life_period or 180
        self.iteration_params = []  # To store parameters at each iteration.

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Fit the model to the input dataframe.
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Predict using the fitted model.
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        # Get the current fit state of the model.
        fit_state = {"params": self.iteration_params}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]) -> None:
        # Set the fit state of the model.
        self.params = fit_state["params"][-1]

    def _preprocess_df(
        self, df_in: pd.DataFrame, half_life_period: int
    ) -> pd.DataFrame:
        """
        Preprocess the input dataframe for the BivariatePoissonModel.

        :param df_in: input dataframe containing match data
        :param half_life_period: half-life period for time weighting
        :return: preprocessed dataframe
        """
        # Calculate the number of days ago each match was played.
        df_in["Days_Ago"] = (df_in["Date"].max() - df_in["Date"]).dt.days
        # Calculate time weights.
        df_in["Time_Weight"] = 0.5 ** (df_in["Days_Ago"] / half_life_period)
        # Return the processed dataframe.
        return df_in

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool
    ) -> Dict[str, pd.DataFrame]:
        if fit:
            data = self._preprocess_df(df_in, self.half_life_period)
            # Initialize parameters for optimization.
            num_teams = int(
                max(data["home_team_id"].max(), data["opponent_id"].max()) + 1
            )
            initial_params = [0, 0, 1] + [0] * num_teams
            # Create the options dictionary conditionally including maxiter.
            options = {"disp": False}
            if self.maxiter is not None:
                options["maxiter"] = self.maxiter
            # Minimize the negative log likelihood.
            result = spop.minimize(
                self._bivariate_poisson_log_likelihood,
                initial_params,
                args=(data,),
                method="L-BFGS-B",
                options=options,
                callback=lambda xk: self._store_and_log_iteration_params(xk),
            )
            # Store the optimized parameters.
            self.params = result.x
        hdbg.dassert_ne(
            0, len(self.params), "Model not found! Check if `fit()` has been run."
        )
        # Calculate match outcomes.
        df_out = rasoprut.get_outcome_probability(df_in, self.params)
        # Apply column mode to the output dataframe.
        df_out = self._apply_col_mode(
            df_in,
            df_out,
            cols=["goals_scored_by_home_team", "goals_scored_by_opponent"],
            col_mode=self._col_mode,
        )
        # Store information about the model and output dataframe.
        info = collections.OrderedDict()
        info["model_params"] = self.params
        info["df_out_info"] = dtfcorutil.get_df_info_as_string(df_out)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        return {"df_out": df_out}

    def _store_and_log_iteration_params(self, params: np.ndarray) -> None:
        """
        Store parameters at each iteration and log them.

        :param params: Current parameters of the optimizer.
        """
        self.iteration_params.append(params.copy())
        _LOG.info(f"Current params: {params}")

    def _bivariate_poisson_log_likelihood(
        self, params: np.ndarray, data: pd.DataFrame
    ) -> float:
        """
        Calculate the negative log likelihood for the bivariate Poisson model.

        :param params: model parameters including team strengths and
            other factors
        :param data: dataframe with the data
        :return: negative log likelihood
        """
        c, h, rho, *strengths = params
        log_likelihood = 0
        for _, row in data.iterrows():
            # Extract and convert necessary variables.
            i, j, goals_i, goals_j, time_weight = (
                int(row["home_team_id"]),
                int(row["opponent_id"]),
                int(row["goals_scored_by_home_team"]),
                int(row["goals_scored_by_opponent"]),
                row["Time_Weight"],
            )
            # Check for out-of-bounds indices.
            if i >= len(strengths) or j >= len(strengths):
                print(
                    f"Index out of bounds: i={i}, j={j}, len(strengths)={len(strengths)}"
                )
                continue
            # Calculate lambda values.
            lambda_i = np.exp(c + strengths[i] + h)
            lambda_j = np.exp(c + strengths[j] - h)
            joint_prob = 0
            # Compute joint probability.
            for k in range(min(goals_i, goals_j) + 1):
                P_goals_i = rasoprut.poisson_probability(lambda_i, goals_i)
                P_goals_j = rasoprut.poisson_probability(lambda_j, goals_j)
                joint_prob += P_goals_i * P_goals_j
            log_likelihood += time_weight * np.log(joint_prob)
        return -log_likelihood
