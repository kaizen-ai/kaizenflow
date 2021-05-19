import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as casgen
import core.config_builders as ccbuild
import helpers.unit_test as hut
from core.dataflow.nodes.transformers import MultiindexSeriesTransformer

_LOG = logging.getLogger(__name__)


class TestMultiindexSeriesTransformer(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = MultiindexSeriesTransformer("sklearn", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=342)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=134
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        realization = np.exp(0.1 * realization.cumsum())
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["close", "volume"])
        return data
