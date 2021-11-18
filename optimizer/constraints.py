import logging
from typing import List

import cvxpy as cvx

import core.config as cconfig
import helpers.dbg as hdbg
import optimizer.base as opba

_LOG = logging.getLogger(__name__)


class ConstraintBuilder:
    """
    Consume a config, e.g.,
    {
        "constraint1": {
            "class": "ConcentrationConstraint",
            "concentration_bound": 0.1,
        },
        "constraint2": {
            "class": "DollarNeutrality",
        },
    }
    and return a list of constraint classes.
    """

    @staticmethod
    def build_constraints(config: cconfig.Config) -> List[opba.Expression]:
        constraints = []
        config_dict = config.to_dict()
        for k, v in config_dict.items():
            _LOG.info("Processing cost=`%s`", k)
            constraint_class_name = config[k]["class"]
            # TODO: Do this another way.
            class_obj = eval(constraint_class_name)
            constraint = class_obj.init_from_config(config[k])
            constraints.append(constraint)
        return constraints


# #############################################################################
# Hard constraints.
# #############################################################################


class ConcentrationConstraint(opba.Expression):
    """
    Restrict max weight in an asset.
    """

    def __init__(self, concentration_bound: float) -> None:
        hdbg.dassert_lte(0, concentration_bound)
        self._concentration_bound = concentration_bound

    @classmethod
    def init_from_config(
        cls, config: cconfig.Config
    ) -> "ConcentrationConstraint":
        config.check_params(["concentration_bound"])
        constraint = cls(config["concentration_bound"])
        return constraint

    def get_expr(self, target_weights, target_diffs, total_wealth) -> opba.EXPR:
        _ = target_diffs
        _ = total_wealth
        # TODO: Normalize this?
        return cvx.max(cvx.abs(target_weights[:-1])) <= self._concentration_bound


class DollarNeutralityConstraint(opba.Expression):
    """
    Require sum of longs weights equals sum of shorts weights.
    """

    @classmethod
    def init_from_config(
        cls, config: cconfig.Config
    ) -> "DollarNeutralityConstraint":
        _ = config
        constraint = cls()
        return constraint

    def get_expr(self, target_weights, target_diffs, total_wealth) -> opba.EXPR:
        _ = target_diffs
        _ = total_wealth
        return sum(target_weights[:-1]) == 0


class LeverageConstraint(opba.Expression):
    """
    Restrict money allocated to non-cash assets.
    """

    def __init__(self, leverage_bound: float) -> None:
        hdbg.dassert_lte(0, leverage_bound)
        self._leverage_bound = leverage_bound

    @classmethod
    def init_from_config(cls, config: cconfig.Config) -> "LeverageConstraint":
        config.check_params(["leverage_bound"])
        constraint = cls(config["leverage_bound"])
        return constraint

    def get_expr(self, target_weights, target_diffs, total_wealth) -> opba.EXPR:
        _ = target_diffs
        _ = total_wealth
        return cvx.norm(target_weights[:-1], 1) <= self._leverage_bound


# TODO: Add no trade, no increase constraints.
# TODO: Add minimum cash balance constraint.
# TODO: Add hard turnover constraint.
