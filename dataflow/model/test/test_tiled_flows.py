import datetime
import logging
import os

import pandas as pd
import pytest

import core.finance_data_example as cfidaexa
import dataflow.model.tiled_flows as dtfmotiflo
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

@pytest.mark.requires_docker
class Test_evaluate_weighted_forecasts(hunitest.TestCase):
    @staticmethod
    def convert_to_parquet_format(df: pd.DataFrame) -> pd.DataFrame:
        df = df.stack()
        df.index.names = ["end_ts", "asset_id"]
        df = df.reset_index(level=1)
        df["year"] = df.index.year
        df["month"] = df.index.month
        return df

    def test_combine_two_signals(self) -> None:
        base_dir = self.get_scratch_space()
        start_datetime = pd.Timestamp(
            "2021-12-20 09:30:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp("2022-01-10 16:00:00", tz="America/New_York")
        asset_ids = [100, 200, 300, 400]
        # Generate strategy 1.
        df1 = cfidaexa.get_forecast_price_based_dataframe(
            start_datetime,
            end_datetime,
            asset_ids,
            bar_duration="30T",
            seed=10,
        )
        _LOG.info("df1=\n%s", df1)
        df1 = self.convert_to_parquet_format(df1)
        _LOG.info("df1=\n%s", df1)
        dir1 = os.path.join(base_dir, "sim1")
        hparque.to_partitioned_parquet(
            df1, ["asset_id", "year", "month"], dst_dir=dir1
        )
        # Generate strategy 2.
        df2 = cfidaexa.get_forecast_price_based_dataframe(
            start_datetime, end_datetime, asset_ids, bar_duration="30T", seed=20
        )
        _LOG.info("df2=\n%s", df2)
        df2 = self.convert_to_parquet_format(df2)
        _LOG.info("df2=\n%s", df2)
        dir2 = os.path.join(base_dir, "sim2")
        hparque.to_partitioned_parquet(
            df2, ["asset_id", "year", "month"], dst_dir=dir2
        )
        # Initialize dataframe pointing to the simulations.
        simulations = pd.DataFrame(
            [[dir1, "prediction"], [dir2, "prediction"]],
            ["sim1", "sim2"],
            ["dir_name", "prediction_col"],
        )
        # Initialize dataframe of weights.
        weights = pd.DataFrame(
            [[1.0, 0.0, 0.5], [0.0, 1.0, 0.5]],
            ["sim1", "sim2"],
            ["sim1", "sim2", "50-50"],
        )
        # Initialize dataframe pointing to price and volatility.
        data = pd.DataFrame(
            [[dir1, "price"], [dir1, "volatility"]],
            ["price", "volatility"],
            ["dir_name", "col"],
        )
        # Compute the resulting metrics.
        bar_metrics = dtfmotiflo.evaluate_weighted_forecasts(
            simulations,
            weights,
            data,
            datetime.date(2021, 12, 20),
            datetime.date(2022, 1, 10),
            "asset_id",
            asset_ids=None,
            annotate_forecasts_kwargs={
                "liquidate_at_end_of_day": False,
            },
            target_freq_str="30T",
            preapply_gaussian_ranking=True,
        )
        actual = hpandas.df_to_str(bar_metrics, num_rows=10, precision=2)
        expected = r"""
                              sim1                                                  sim2                                                 50-50
                               pnl gross_volume net_volume       gmv        nmv      pnl gross_volume net_volume       gmv        nmv      pnl gross_volume net_volume       gmv        nmv
end_ts
2021-12-20 12:00:00-05:00  -673.68     4.77e+05 -292609.33  9.99e+05  497751.84  -734.18     4.77e+05  -2.92e+05  9.99e+05  500200.62  -673.68     7.26e+05 -290160.55  9.99e+05  500200.62
2021-12-20 12:30:00-05:00 -1084.75     2.00e+06 -641598.62  1.00e+06 -144931.53   477.90     1.50e+06  -3.64e+05  1.00e+06  136774.80   477.90     1.38e+06 -645610.05  1.00e+06 -144931.53
2021-12-20 13:00:00-05:00  -247.31     1.43e+06  956486.47  1.00e+06  811307.62  -551.75     2.00e+06   6.75e+05  1.00e+06  811307.62  -247.31     1.43e+06  956486.47  1.00e+06  811307.62
2021-12-20 13:30:00-05:00  1079.62     1.17e+06 -850069.32  1.00e+06  -37682.08  1079.62     2.64e+05  -1.42e+05  1.00e+06  670238.89  1079.62     2.64e+05 -142148.35  1.00e+06  670238.89
2021-12-20 14:00:00-05:00  -125.20     3.63e+05 -244433.37  1.00e+06 -282240.66   448.75     2.00e+06  -1.13e+06  1.00e+06 -460021.45   448.75     1.52e+06 -653303.21  1.00e+06   17384.42
...
end_ts
2022-01-10 14:00:00-05:00  404.09     2.00e+06  521638.52  1.00e+06  309905.28  404.09     8.08e+05  183340.49  1.00e+06  -28392.76   404.09     2.00e+06  521638.52  1.00e+06  309905.28
2022-01-10 14:30:00-05:00  654.69     8.79e+05 -316012.94  1.00e+06   -5452.98  248.76     1.35e+06  149603.38  1.00e+06  121459.37   654.69     1.03e+06 -189100.59  1.00e+06  121459.37
2022-01-10 15:00:00-05:00  849.07     2.00e+06 -205502.75  1.00e+06 -210106.67  572.70     8.98e+05   88074.59  1.00e+06  210106.67   572.70     2.00e+06 -413183.39  1.00e+06 -291151.32
2022-01-10 15:30:00-05:00 -912.96     1.27e+06  270448.98  1.00e+06   59429.36  912.96     1.27e+06 -270448.98  1.00e+06  -59429.36 -1127.83     1.27e+06  255320.69  1.00e+06  -36958.46
2022-01-10 16:00:00-05:00  847.38     1.14e+06 -368085.80  9.99e+05 -307809.05 -847.38     1.31e+06   79002.84  9.99e+05   18726.10  -930.35     1.59e+06  464871.17  9.99e+05  426982.36"""
        self.assert_equal(actual, expected, fuzzy_match=True)
