import cvxpy as cvx
import pandas as pd

import core.config as cconfig
import helpers.dbg as hdbg
import optimizer.constraints as opcon
import optimizer.costs as opcos

# #############################################################################
# Single period optimization with costs and constraints
# #############################################################################


class SinglePeriodOptimizer:
    def __init__(
        self,
        costs,
        constraints,
    ) -> None:
        """
        Initialize with cost and constraint functions.

        Cost and constraint functions may require state.
        Costs may be multiplied by tuneable parameters when passed in.
        TODO: Write a builder to generate costs and constraints from files.

        :param costs:
        :param constraints:
        """
        self._costs = costs
        self._constraints = constraints

    def optimize(
        self,
        holdings: pd.Series,
        predictions: pd.Series,
    ) -> pd.DataFrame:
        """
        Get target positions (in units of money).

        :param holdings:
        :param predictions:
        :return:
        """
        self._initialize_vars(holdings, predictions)
        costs = self._materialize(self._costs)
        constraints = self._materialize(self._constraints)
        # Add a self-financing constraint.
        constraints.append(cvx.sum(self._target_diffs) == 0)
        # Create the cvxpy problem.
        problem = cvx.Problem(cvx.Maximize(self._mu - sum(costs)), constraints)
        self._optimal_value = problem.solve()
        hdbg.dassert_eq(problem.status, "optimal")
        result_df = self._process_results()
        # TODO: process the results
        return result_df

    def generate_stats(self) -> pd.Series:
        dict_ = {
            "optimal_value": self._optimal_value,
            "mu": self._mu.expr.value,
        }
        for cost in self._costs:
            # TODO: Check for dups
            dict_[type(cost).__name__] = cost.expr.value / cost.gamma.value
        return pd.Series(dict_, name="stats")

    def _initialize_vars(
        self, holdings: pd.Series, predictions: pd.Series
    ) -> None:
        hdbg.dassert_isinstance(holdings, pd.Series)
        hdbg.dassert_isinstance(predictions, pd.Series)
        hdbg.dassert_eq(holdings.size, predictions.size)
        self._index = holdings.index
        self._size = holdings.size
        #
        self._total_wealth = holdings.sum()
        self._current_weights = holdings / self._total_wealth
        self._target_diffs = cvx.Variable(self._size)
        self._target_weights = self._current_weights.values + self._target_diffs
        #
        self._rets = cvx.multiply(predictions.values, self._target_weights)
        self._mu = cvx.sum(self._rets)
        hdbg.dassert(self._mu.is_concave())

    def _materialize(self, expressions) -> list:
        """

        :param expressions: a cost or constraint with `get_expr()`
        :return:
        """
        materialized = []
        for expr in expressions:
            expr = expr.get_expr(
                self._target_weights, self._target_diffs, self._total_wealth
            )
            materialized.append(expr)
        return materialized

    def _process_results(self) -> pd.DataFrame:
        # Collect series.
        srs_list = []
        # Target positions (notional).
        target_positions = pd.Series(
            index=self._index,
            data=self._target_weights.value * self._total_wealth,
            name="target_positions",
        )
        srs_list.append(target_positions)
        # Target trades (notional).
        target_trades = pd.Series(
            index=self._index,
            data=self._target_diffs.value * self._total_wealth,
            name="target_trades",
        )
        srs_list.append(target_trades)
        # Target weights.
        target_weights = pd.Series(
            index=self._index,
            data=self._target_weights.value,
            name="target_weights",
        )
        srs_list.append(target_weights)
        # Target weight adjustments.
        target_weight_diffs = pd.Series(
            index=self._index,
            data=self._target_diffs.value,
            name="target_weight_diffs",
        )
        srs_list.append(target_weight_diffs)
        # TODO: Add a layer somewhere to postprocess this df and generate
        #   - per-side GMV
        #   - total exposure
        #   - leverage
        #   - turnover
        return pd.concat(srs_list, axis=1)


def perform_single_period_optimization(
    config: cconfig.Config,
) -> None:
    config.check_params(
        ["costs", "constraints", "holdings", "predictions", "result_path"]
    )
    costs = opcos.CostBuilder.build_costs(config["costs"])
    constraints = opcon.ConstraintBuilder.build_constraints(config["constraints"])
    spo = SinglePeriodOptimizer(costs, constraints)
    holdings = pd.read_csv(config["holdings"], index_col=0).squeeze()
    predictions = pd.read_csv(config["predictions"], index_col=0).squeeze()
    df = spo.optimize(holdings, predictions)
    df.to_csv(config["result_path"])
