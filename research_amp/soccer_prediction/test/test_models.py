import logging

import numpy as np
import pandas as pd

import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sklearn_models as dtfcnoskmo
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import research_amp.soccer_prediction.models as rasoprmo

_LOG = logging.getLogger(__name__)


class TestBivariatePoissonWrapper(hunitest.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = pd.DataFrame(
            {
                "HT_id": np.random.randint(0, 10, 1000),
                "AT_id": np.random.randint(0, 10, 1000),
                "HS": np.random.poisson(1.5, 1000),
                "AS": np.random.poisson(1.5, 1000),
                "Time_Weight": np.random.uniform(0.8, 1.2, 1000),
                "Lge": ["ENG5"] * 1000,
                "Sea": np.random.choice(["07-08", "06-07", "08-09"], 1000),
            }
        )
        # Split into features and target.
        X = data[["HT_id", "AT_id", "Time_Weight"]]
        data[["HS", "AS"]]
        # Define the model function.
        model_func = lambda: rasoprmo.BivariatePoissonWrapper(maxiter=1)
        # Define node ID and variables.
        node_id = dtfcornode.NodeId("poisson_regressor")
        x_vars = X.columns.tolist()
        y_vars = ["HS", "AS"]
        steps_ahead = 1
        # Instantiate the ContinuousSkLearnModel with the bivariate Poisson wrapper.
        poisson_model_node = dtfcnoskmo.ContinuousSkLearnModel(
            nid=node_id,
            model_func=model_func,
            x_vars=x_vars,
            y_vars=y_vars,
            steps_ahead=steps_ahead,
        )
        # Check the output.
        df_out = poisson_model_node.fit(data)["df_out"]
        df_str = hpandas.df_to_str(df_out.round(3), num_rows=None, precision=3)
        self.check_string(df_str, fuzzy_match=True)
