"""
Import as:

import defi.dao_cross.optimize as ddacropt
"""

import logging
from typing import Any, Dict

import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg
import helpers.hprint as hprint

# Equivalent to `import x as x`, but skip this module if the module is
# not present.
import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

pulp = pytest.importorskip("pulp")

_LOG = logging.getLogger(__name__)


# TODO(Grisha): consider extending for n orders.
def run_solver(
    order_1: ddacrord.Order, order_2: ddacrord.Order, exchange_rate: float
) -> Dict[str, Any]:
    """
    Find the maximum transacted volume given the constraints.

    :param order_1: buy order
    :param order_2: sell order
    :param exchange_rate: ratio -- price of base token / price of quote token
    :return: solver's output in a human readable format
    """
    _LOG.debug(hprint.to_str("order_1 order_2"))
    # Assume the fixed directions.
    hdbg.dassert_eq(order_1.action, "buy")
    hdbg.dassert_eq(order_2.action, "sell")
    #
    hdbg.dassert_lt(0, exchange_rate)
    # Initialize the model.
    problem = pulp.LpProblem("The DaoCross problem", pulp.LpMaximize)
    # Specify the vars. Setting the lower bound to zero allows
    # to omit the >= 0 constraint on the executed quantities.
    q_base_asterisk_1 = pulp.LpVariable("q_base_asterisk_1", lowBound=0)
    q_base_asterisk_2 = pulp.LpVariable("q_base_asterisk_2", lowBound=0)
    # Objective function.
    # TODO(Grisha): since the base token is the same, i.e. BTC it's
    # ok to use quantity, however the objective function should be
    # modified to account for different base tokens.
    problem += q_base_asterisk_1 + q_base_asterisk_2
    # Constraints.
    # Random number that is big enough to use the "Big M" method.
    M = 1e6
    # TODO(Grisha): this should be a function of action.
    limit_price_cond_1 = int(exchange_rate <= order_1.limit_price)
    _LOG.debug(hprint.to_str("limit_price_cond_1"))
    limit_price_cond_2 = int(exchange_rate >= order_2.limit_price)
    _LOG.debug(hprint.to_str("limit_price_cond_2"))
    # Executed quantity is not greater than the requested quantity
    # given that the limit price condition is satisfied.
    problem += q_base_asterisk_1 <= order_1.quantity + M * (
        1 - limit_price_cond_1
    )
    problem += q_base_asterisk_2 <= order_2.quantity + M * (
        1 - limit_price_cond_2
    )
    # Executed quantity is zero if the limit price condition is not met.
    problem += q_base_asterisk_1 <= M * limit_price_cond_1
    problem += q_base_asterisk_1 >= -M * limit_price_cond_1
    #
    problem += q_base_asterisk_2 <= M * limit_price_cond_2
    problem += q_base_asterisk_2 >= -M * limit_price_cond_2
    # The number of sold tokens must match the number of bought tokens.
    problem += q_base_asterisk_1 == q_base_asterisk_2
    # Use the default solver and suppress the solver's log.
    solver = pulp.getSolver("PULP_CBC_CMD", msg=0)
    problem.solve(solver)
    # Display the results.
    # TODO(Grisha): move packaging to a separate function.
    result: Dict[str, Any] = {}
    result["problem_status"] = pulp.LpStatus[problem.status]
    result["problem_objective_value"] = pulp.value(problem.objective)
    # TODO(Grisha): handle N orders, not just 2.
    result["q_base_asterisk_1"] = q_base_asterisk_1.varValue
    result["q_base_asterisk_2"] = q_base_asterisk_2.varValue
    # TODO(Grisha): double-check that time is in seconds.
    result["solution_time_in_secs"] = round(problem.solutionTime, 2)
    return result
