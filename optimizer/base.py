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
        target_diffs,
        total_wealth: float,
    ) -> EXPR:
        """

        :param target_weights: current weights plus target diffs
        :param target_diffs: trades normalized by `value` (of holdings); same
            scale as weights
        :param total_wealth: portfolio in notional plus cash balance
        :return: cvxpy Expression
        """
        ...
