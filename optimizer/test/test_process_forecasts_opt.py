import asyncio
import datetime
import logging
import os
from typing import Tuple

import pandas as pd
import pytest

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hunit_test as hunitest
import market_data as mdata

# TODO(gp): Use import oms
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore

_LOG = logging.getLogger(__name__)


class TestDataFrameProcessForecasts1(hunitest.TestCase):
    @pytest.mark.skip("Generate manually files used by other tests")
    def test_generate_data(self) -> None:
        """
        Generate market data, predictions, and volatility files used as inputs
        by other tests.

        This test might need to be run from an `amp` container.
        """
        import core.finance_data_example as cfidaexa

        dir_ = self.get_input_dir()
        hio.create_dir(dir_, incremental=False)
        # Market data.
        start_datetime = pd.Timestamp(
            "2000-01-01 09:35:00-05:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp(
            "2000-01-01 10:30:00-05:00", tz="America/New_York"
        )
        asset_ids = [100, 200]
        market_data_df = mdata.generate_random_bars(
            start_datetime, end_datetime, asset_ids
        )
        market_data_df.to_csv(os.path.join(dir_, "market_data_df.csv"))
        # Prediction.
        forecasts = cfidaexa.get_forecast_dataframe(
            start_datetime + pd.Timedelta("5T"), end_datetime, asset_ids
        )
        prediction_df = forecasts["prediction"]
        prediction_df.to_csv(os.path.join(dir_, "prediction_df.csv"))
        # Volatility.
        volatility_df = forecasts["volatility"]
        volatility_df.to_csv(os.path.join(dir_, "volatility_df.csv"))

    # ///////////////////////////////////////////////////////////////////////////

    def get_input_filename(self, filename: str) -> str:
        dir_ = self.get_input_dir(test_method_name="test_generate_data")
        filename = os.path.join(dir_, filename)
        hdbg.dassert_file_exists(filename)
        return filename

    def get_market_data(self, event_loop) -> mdata.MarketData:
        filename = self.get_input_filename("market_data_df.csv")
        market_data_df = pd.read_csv(
            filename,
            index_col=0,
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        market_data_df = market_data_df.convert_dtypes()
        initial_replayed_delay = 5
        market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
            event_loop,
            initial_replayed_delay,
            market_data_df,
        )
        return market_data

    def get_predictions_and_volatility(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        filename = self.get_input_filename("prediction_df.csv")
        prediction_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        prediction_df = prediction_df.convert_dtypes()
        prediction_df.index = prediction_df.index.tz_convert("America/New_York")
        prediction_df.columns = prediction_df.columns.astype("int64")
        #
        filename = self.get_input_filename("volatility_df.csv")
        volatility_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        volatility_df = volatility_df.convert_dtypes()
        volatility_df.index = volatility_df.index.tz_convert("America/New_York")
        volatility_df.columns = volatility_df.columns.astype("int64")
        return prediction_df, volatility_df

    # TODO(gp): This can become an _example.
    def get_portfolio(
        self,
        event_loop,
    ) -> omportfo.DataFramePortfolio:
        market_data = self.get_market_data(event_loop)
        asset_ids = market_data._asset_ids
        strategy_id = "strategy"
        account = "account"
        timestamp_col = "end_datetime"
        mark_to_market_col = "close"
        pricing_method = "last"
        initial_holdings = pd.Series([-50, 50, 0], asset_ids + [-1])
        column_remap = {
            "bid": "bid",
            "ask": "ask",
            "price": "close",
            "midpoint": "midpoint",
        }
        portfolio = oporexam.get_DataFramePortfolio_example2(
            strategy_id,
            account,
            market_data,
            timestamp_col,
            mark_to_market_col,
            pricing_method,
            initial_holdings,
            column_remap=column_remap,
        )
        return portfolio

    # TODO(gp): This can become an _example.
    @staticmethod
    def get_process_forecasts_config() -> cconfig.Config:
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "batch_optimizer",
                "dollar_neutrality_penalty": 0.1,
                "volatility_penalty": 0.5,
                "turnover_penalty": 0.0,
                "target_gmv": 1e5,
                "target_gmv_upper_bound_multiple": 1.0,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    async def run_process_forecasts(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        predictions, volatility = self.get_predictions_and_volatility()
        portfolio = self.get_portfolio(event_loop)
        config = self.get_process_forecasts_config()
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        actual = str(portfolio)
        expected = r"""
# historical holdings=
asset_id                     100    200    -1
2000-01-01 09:40:01-05:00 -50.00  50.00    0.00
2000-01-01 09:45:01-05:00 -50.02  49.83  193.42
2000-01-01 09:50:01-05:00 -50.03  49.96   68.45
2000-01-01 09:55:01-05:00 -50.53  50.26  263.78
2000-01-01 10:00:01-05:00 -50.28  49.78  504.97
2000-01-01 10:05:01-05:00 -50.68  50.25  423.72
2000-01-01 10:10:01-05:00 -50.35  50.17  184.96
2000-01-01 10:15:01-05:00 -50.49  50.16  334.15
2000-01-01 10:20:01-05:00  50.41 -50.18  359.94
2000-01-01 10:25:01-05:00  50.34 -50.09  334.00
2000-01-01 10:30:01-05:00 -50.28  50.20   68.77
# historical holdings marked to market=
asset_id                        100       200     -1
2000-01-01 09:40:01-05:00 -49870.58  50090.60    0.00
2000-01-01 09:45:01-05:00 -49981.30  49859.86  193.42
2000-01-01 09:50:01-05:00 -49845.78  50077.47   68.45
2000-01-01 09:55:01-05:00 -50219.50  50458.24  263.78
2000-01-01 10:00:01-05:00 -49860.71  49745.89  504.97
2000-01-01 10:05:01-05:00 -50266.02  50019.65  423.72
2000-01-01 10:10:01-05:00 -49846.88  49991.49  184.96
2000-01-01 10:15:01-05:00 -50100.10  49991.53  334.15
2000-01-01 10:20:01-05:00  49984.34 -50006.16  359.94
2000-01-01 10:25:01-05:00  49938.40 -49778.45  334.00
2000-01-01 10:30:01-05:00 -49835.69  50095.28   68.77
# historical flows=
asset_id                         100        200
2000-01-01 09:45:01-05:00      18.28     175.14
2000-01-01 09:50:01-05:00      11.08    -136.05
2000-01-01 09:55:01-05:00     494.89    -299.57
2000-01-01 10:00:01-05:00    -241.37     482.55
2000-01-01 10:05:01-05:00     388.14    -469.39
2000-01-01 10:10:01-05:00    -318.07      79.30
2000-01-01 10:15:01-05:00     135.87      13.32
2000-01-01 10:20:01-05:00 -100045.72  100071.51
2000-01-01 10:25:01-05:00      69.11     -95.05
2000-01-01 10:30:01-05:00   99710.19  -99975.41
# historical pnl=
asset_id                      100     200
2000-01-01 09:40:01-05:00     NaN     NaN
2000-01-01 09:45:01-05:00  -92.44  -55.60
2000-01-01 09:50:01-05:00  146.60   81.55
2000-01-01 09:55:01-05:00  121.18   81.21
2000-01-01 10:00:01-05:00  117.42 -229.79
2000-01-01 10:05:01-05:00  -17.16 -195.63
2000-01-01 10:10:01-05:00  101.07   51.14
2000-01-01 10:15:01-05:00 -117.34   13.36
2000-01-01 10:20:01-05:00   38.72   73.82
2000-01-01 10:25:01-05:00   23.17  132.65
2000-01-01 10:30:01-05:00  -63.90 -101.68
# historical statistics=
                              pnl  gross_volume  net_volume        gmv     nmv    cash  net_wealth  leverage
2000-01-01 09:40:01-05:00     NaN          0.00        0.00   99961.18  220.02    0.00      220.02    454.32
2000-01-01 09:45:01-05:00 -148.04        193.42     -193.42   99841.17 -121.44  193.42       71.98   1387.03
2000-01-01 09:50:01-05:00  228.16        147.13      124.97   99923.25  231.68   68.45      300.14    332.92
2000-01-01 09:55:01-05:00  202.38        794.46     -195.32  100677.74  238.74  263.78      502.52    200.34
2000-01-01 10:00:01-05:00 -112.37        723.92     -241.19   99606.60 -114.82  504.97      390.15    255.30
2000-01-01 10:05:01-05:00 -212.79        857.53       81.25  100285.67 -246.36  423.72      177.36    565.44
2000-01-01 10:10:01-05:00  152.21        397.37      238.76   99838.37  144.61  184.96      329.57    302.94
2000-01-01 10:15:01-05:00 -103.99        149.19     -149.19  100091.63 -108.57  334.15      225.58    443.71
2000-01-01 10:20:01-05:00  112.54     200117.23      -25.79   99990.50  -21.82  359.94      338.12    295.72
2000-01-01 10:25:01-05:00  155.82        164.17       25.94   99716.85  159.94  334.00      493.94    201.88
2000-01-01 10:30:01-05:00 -165.58     199685.60      265.23   99930.98  259.59   68.77      328.36    304.33
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    # TODO(gp): -> test1
    def test_initialization1(self) -> None:
        """
        Run the process forecasts.
        """
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self.run_process_forecasts(event_loop), event_loop=event_loop
            )
