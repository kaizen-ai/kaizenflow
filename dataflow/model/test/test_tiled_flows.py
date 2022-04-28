import datetime
import logging
import os

import pandas as pd

import core.finance_data_example as cfidaexa
import dataflow.model.tiled_flows as dtfmotiflo
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


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
            annotate_forecasts_kwargs=None,
            target_freq_str="30T",
        )
        actual = hpandas.df_to_str(bar_metrics, num_rows=10, precision=2)
        expected = r"""                             
                              sim1                                               sim2                                              50-50                                           
                              pnl gross_volume net_volume        gmv     nmv     pnl gross_volume net_volume        gmv     nmv     pnl gross_volume net_volume        gmv     nmv
end_ts                                                                                                                                                                            
2021-12-20 10:00:00-05:00     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN
2021-12-20 10:30:00-05:00     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN
2021-12-20 11:00:00-05:00     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN     NaN          NaN        NaN        NaN     NaN
2021-12-20 11:30:00-05:00  187.55    151683.12      38.87  999413.53  167.47  -45.34     1.85e+06    -407.73  999765.56   71.82  147.65    998560.89    -389.92  999413.53  167.47
2021-12-20 12:00:00-05:00  -84.95         0.00       0.00  998692.88   82.52 -299.06     1.69e+06    -380.62  999326.83 -607.86  -84.95    999425.82    -552.22  999464.99 -469.70
...
end_ts                                                                                                                                                                              
2022-01-10 14:00:00-05:00   290.74     2.00e+06    -329.53  1.00e+06  -850.55   759.03     1.85e+06    1096.74  9.99e+05   -61.84   759.03     2.00e+06     308.02  1.00e+06 -850.55
2022-01-10 14:30:00-05:00   494.01     1.00e+06     492.40  1.00e+06   135.86   875.68     1.85e+06   -1451.54  1.00e+06  -637.70   494.01     1.00e+06    -251.69  1.00e+06 -608.23
2022-01-10 15:00:00-05:00   991.62     2.00e+06   -3195.99  1.00e+06 -2068.50   119.49     1.00e+06    2144.86  1.00e+06  1626.64   931.10     2.00e+06    -176.03  1.00e+06  146.84
2022-01-10 15:30:00-05:00 -1568.34     1.85e+06    4565.56  1.00e+06   928.72   768.81     1.00e+06   -3175.92  1.00e+06  -780.47  -956.01     1.00e+06    1282.36  9.99e+05  473.19
2022-01-10 16:00:00-05:00    79.23     1.00e+06   -1592.03  1.00e+06  -584.09 -1115.89     9.99e+05    2990.87  1.00e+06  1094.52 -1186.80     2.00e+06    1385.63  9.99e+05  672.02"""
        self.assert_equal(actual, expected, fuzzy_match=True)
