import logging
import pandas as pd

import core.artificial_signal_generators as casgen
import core.dataflow.nodes.local_level_model as cdnllm
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

class TestLocalLevelModel(hut.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = self._get_data(1)
        # Generate node config.
        node = cdnllm.LocalLevelModel(
            "llm",
            col=["close"],
            col_mode="merge_all",
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def _get_data(self, lag: int) -> pd.DataFrame:
        """
        Generate "random returns".
        """
        arma_process = casgen.ArmaProcess([0], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=10
        )
        realization = realization.cumsum()
        realization.name = "close"
        df = pd.DataFrame(index=date_range, data=realization)
        return df
