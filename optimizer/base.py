"""
Import as:

import optimizer.base as opbase
"""

import abc

import cvxpy as cvx

EXPR = cvx.expressions.expression.Expression


# #############################################################################
# Base `Expression` class.
# #############################################################################


class Expression(abc.ABC):
    @abc.abstractmethod
    def get_expr(
        self,
        target_weights,
        target_weight_diffs,
        gmv: float,
    ) -> EXPR:
        """

        :param target_weights: current weights plus target weight diffs
        :param target_weight_diffs: trades normalized by current gmv. These
            are on the same scale as the weights.
        :param gmv: sum of absolute value of notional value of assets
        :return: cvxpy Expression
        """
        ...
