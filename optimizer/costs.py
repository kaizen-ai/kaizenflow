"""
Import as:

import optimizer.costs as opcosts
"""

import abc
import logging
from typing import List

import cvxpy as cvx
import numpy as np
import pandas as pd

import core.config as cconfig
import helpers.dbg as hdbg
import optimizer.base as opbase
import optimizer.utils as oputils

_LOG = logging.getLogger(__name__)


# #############################################################################
# Class and builder for objective function costs.
# #############################################################################


class Cost(abc.ABC):
    """
    Base class for soft constraints.
    """

    def __init__(self, gamma: float = 1.0) -> None:
        """
        Initialize the Lagrange multiplier to 1.0.
        """
        hdbg.dassert_lte(0, gamma)
        self.gamma = cvx.Parameter(nonneg=True, value=gamma)
        self.expr = None

    def __mul__(self, other):
        """
        Scale the cost by the constant.
        """
        self.gamma.value *= other
        return self

    def __rmul__(self, other):
        """
        Scale the cost by the constant.
        """
        return self.__mul__(other)

    def get_expr(self, target_weights, target_diffs, total_wealth) -> opbase.EXPR:
        expr = self._estimate(target_weights, target_diffs, total_wealth)
        self.expr = expr.copy()
        return self.gamma * expr

    @abc.abstractmethod
    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        ...


class CostBuilder:
    """
    Consume a config, e.g.,
    {
        "cost1": {
            "class": "DiagonalRiskModel",
            "path": "path/to/csv",
            "gamma": 1,
        },
        "cost2": {
            "class": "SpreadCost",
            "path": "path/to/csv",
            "gamma": 1,
        },
    }
    and return a list of cost classes.

    This class maps keys to classes.
    Q: Do we ever need to use the same class more than once in a given config?
    """

    @staticmethod
    def build_costs(config: cconfig.Config) -> List[Cost]:
        costs = []
        config_dict = config.to_dict()
        for k, v in config_dict.items():
            _LOG.info("Processing cost=`%s`", k)
            cost_class_name = config[k]["class"]
            # TODO: Do this another way.
            class_obj = eval(cost_class_name)
            cost = class_obj.init_from_config(config[k])
            costs.append(cost)
        return costs


# #############################################################################
# Risk models.
# #############################################################################


class DiagonalRiskModel(Cost):
    """
    Impose a diagonal risk cost.
    """

    def __init__(self, risk: pd.Series, gamma: float = 1.0) -> None:
        self._sigma_sqrt = np.sqrt(risk)
        super().__init__(gamma)

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "DiagonalRiskModel":
        config.check_params(["path", "gamma"])
        df = pd.read_csv(config["path"], index_col=0)
        risk = df.squeeze()
        cost = cls(risk, config["gamma"])
        return cost

    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        _ = target_diffs
        _ = total_wealth
        expr = cvx.sum_squares(target_weights.T @ self._sigma_sqrt.values)
        return expr


class CovarianceRiskModel(Cost):
    """
    Utilize a full covariance matrix for the risk model.
    """

    def __init__(self, risk: pd.DataFrame, gamma: float = 1.0) -> None:
        hdbg.dassert(oputils.is_symmetric(risk))
        self._risk = risk
        super().__init__(gamma)

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "DiagonalRiskModel":
        config.check_params(["path", "gamma"])
        risk = pd.read_csv(config["path"], index_col=0)
        cost = cls(risk, config["gamma"])
        return cost

    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        _ = target_diffs
        _ = total_wealth
        expr = cvx.quad_form(target_weights, self._risk.values)
        return expr


# #############################################################################
# Transaction costs.
# #############################################################################


class SpreadCost(Cost):
    def __init__(self, spread: pd.Series, gamma: float = 1.0) -> None:
        hdbg.dassert_isinstance(spread, pd.Series)
        hdbg.dassert((spread >= 0).all())
        self._spread = spread
        super().__init__(gamma)

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "SpreadCost":
        config.check_params(["path", "gamma"])
        df = pd.read_csv(config["path"], index_col=0)
        spread = df.squeeze()
        cost = cls(spread, config["gamma"])
        return cost

    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        _ = target_weights
        _ = total_wealth
        expr = (self._spread.values / 2) @ cvx.abs(target_diffs).T
        return expr


# #############################################################################
# Soft constraints.
# #############################################################################


class DollarNeutralityCost(Cost):
    """
    Impose a cost on violating dollar neutrality.
    """

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "DollarNeutralityCost":
        config.check_params(["gamma"])
        cost = cls(config["gamma"])
        return cost

    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        _ = target_diffs
        _ = total_wealth
        return cvx.abs(sum(target_weights[:-1]))


class TurnoverCost(Cost):
    """
    Impose a cost on long and short turnover.
    """

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "TurnoverCost":
        config.check_params(["gamma"])
        cost = cls(config["gamma"])
        return cost

    def _estimate(
        self, target_weights, target_diffs, total_wealth
    ) -> opbase.EXPR:
        _ = target_weights
        _ = total_wealth
        pos = cvx.norm(cvx.pos(target_diffs[:-1]), 1)
        neg = cvx.norm(cvx.neg(target_diffs[:-1]), 1)
        weight = pos + neg
        return weight
