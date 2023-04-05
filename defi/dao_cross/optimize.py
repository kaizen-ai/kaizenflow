"""
Import as:

import defi.dao_cross.optimize as ddacropt
"""

import logging
from typing import Any, Dict, List

import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg
import helpers.hprint as hprint

# Equivalent to `import x as x`, but skip this module if the module is
# not present.
import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

pulp = pytest.importorskip("pulp")

_LOG = logging.getLogger(__name__)


# TODO(Grisha): consider extending for n base tokens.
def run_solver(
    orders: List[ddacrord.Order], exchange_rate: float
) -> Dict[str, Any]:
    """
    Find the maximum exchanged volume given the constraints.

    :param orders: buy / sell orders
    :param exchange_rate: price (in terms of quote token) per unit of base token
    :return: solver's output in a human readable format
    """
    _LOG.debug(hprint.to_str("orders"))
    _LOG.debug(hprint.to_str("exchange_rate"))
    #
    n_orders = len(orders)
    hdbg.dassert_lt(0, n_orders)
    hdbg.dassert_container_type(orders, list, ddacrord.Order)
    #
    hdbg.dassert_lt(0, exchange_rate)
    # Initialize the model.
    problem = pulp.LpProblem("The DaoCross problem", pulp.LpMaximize)
    # Specify the executed quantities vars. Setting the lower bound to zero
    # allows to omit the >= 0 constraint.
    q_base_asterisk = [
        pulp.LpVariable(f"q_base_asterisk_{i}", lowBound=0)
        for i in range(n_orders)
    ]
    # Objective function.
    # TODO(Grisha): since the base token is the same, i.e. BTC it is ok to use
    # quantity, however the objective function should be modified to account for
    # different base tokens.
    problem += pulp.lpSum(q_base_asterisk)
    # Constraints.
    # Impose constraints on executed quantites on the order level.
    for i in range(n_orders):
        limit_price_cond = exchange_rate * ddacrord.action_to_int(orders[i].action) <= orders[i].limit_price * ddacrord.action_to_int(orders[i].action)
        _LOG.debug(hprint.to_str("limit_price_cond"))
        if limit_price_cond:
            # Executed quantity is less than or equal to the requested quantity.
            problem += q_base_asterisk[i] <= orders[i].quantity
        else:
            # Executed quantity is zero, i.e., the order cannot be executed.
            problem += q_base_asterisk[i] == 0
    # Global constraint: the number of sold tokens must match the number
    # of bought tokens.
    problem += (
        pulp.lpSum(
            q_base_asterisk[i] * ddacrord.action_to_int(orders[i].action)
            for i in range(n_orders)
        )
        == 0
    )
    # Use the default solver and suppress the solver's log.
    solver = pulp.getSolver("PULP_CBC_CMD", msg=0)
    problem.solve(solver)
    # Display the results.
    # TODO(Grisha): move packaging to a separate function.
    result: Dict[str, Any] = {}
    result["problem_status"] = pulp.LpStatus[problem.status]
    result["problem_objective_value"] = pulp.value(problem.objective)
    # TODO(Grisha): maybe store in a dict? e.g., `{order_i: q_base_asterisk_i}`.
    result["q_base_asterisk"] = [var.varValue for var in q_base_asterisk]
    # TODO(Grisha): double-check that time is in seconds.
    result["solution_time_in_secs"] = round(problem.solutionTime, 2)
    return result
