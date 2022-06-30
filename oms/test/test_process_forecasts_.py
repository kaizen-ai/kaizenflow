import asyncio
import datetime
import logging
from typing import List, Tuple, Union

import pandas as pd
import pytest

import core.config as cconfig
import core.finance as cofinanc
import core.finance_data_example as cfidaexa
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.oms_db as oomsdb
import oms.order_processor as oordproc
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts_ as oprofore
import oms.test.oms_db_helper as omtodh

# TODO(gp): Why does this file ends with _.py?


_LOG = logging.getLogger(__name__)


class TestSimulatedProcessForecasts1(hunitest.TestCase):
    @staticmethod
    def get_portfolio(
        event_loop, asset_ids: List[int]
    ) -> omportfo.DataFramePortfolio:
        (
            market_data,
            get_wall_clock_time,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
        portfolio = oporexam.get_DataFramePortfolio_example1(
            event_loop, market_data=market_data, asset_ids=asset_ids
        )
        return portfolio

    @staticmethod
    def get_process_forecasts_config() -> cconfig.Config:
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "pomo",
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        asset_ids = [101, 202]
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, asset_ids)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, asset_ids)
        # Build a Portfolio.
        portfolio = self.get_portfolio(event_loop, asset_ids)
        # Get process forecasts config.
        config = self.get_process_forecasts_config()
        spread_df = None
        restrictions_df = None
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
            spread_df,
            restrictions_df,
        )
        actual = str(portfolio)
        expected = r"""# historical holdings=
asset_id                     101    202       -1
2000-01-01 09:35:01-05:00   0.00   0.00  1000000.0
2000-01-01 09:40:01-05:00 -49.98  49.98  1000000.0
2000-01-01 09:45:01-05:00 -49.99  49.99  1000000.0
# historical holdings marked to market=
asset_id                        101       202       -1
2000-01-01 09:35:01-05:00      0.00      0.00  1000000.0
2000-01-01 09:40:01-05:00 -49994.47  49994.47  1000000.0
2000-01-01 09:45:01-05:00 -49985.86  49985.86  1000000.0
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00  49980.22 -49980.22
2000-01-01 09:45:01-05:00      5.53     -5.53
# historical pnl=
asset_id                     101    202
2000-01-01 09:35:01-05:00    NaN    NaN
2000-01-01 09:40:01-05:00 -14.26  14.26
2000-01-01 09:45:01-05:00  14.14 -14.14
# historical statistics=
                           pnl  gross_volume  net_volume       gmv  nmv       cash  net_wealth  leverage
2000-01-01 09:35:01-05:00  NaN          0.00         0.0      0.00  0.0  1000000.0   1000000.0       0.0
2000-01-01 09:40:01-05:00  0.0      99960.44         0.0  99988.95  0.0  1000000.0   1000000.0       0.1
2000-01-01 09:45:01-05:00  0.0         11.05         0.0  99971.72  0.0  1000000.0   1000000.0       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestSimulatedProcessForecasts2(hunitest.TestCase):
    @staticmethod
    def get_portfolio(
        event_loop,
        start_datetime: pd.Timestamp,
        end_datetime: pd.Timestamp,
        asset_ids: List[int],
    ) -> omportfo.DataFramePortfolio:
        initial_replayed_delay = 5
        delay_in_secs = 0
        columns = ["price"]
        sleep_in_secs = 30
        time_out_in_secs = 60 * 5
        (
            market_data,
            get_wall_clock_time,
        ) = mdata.get_ReplayedTimeMarketData_example2(
            event_loop,
            start_datetime,
            end_datetime,
            initial_replayed_delay,
            asset_ids,
            delay_in_secs=delay_in_secs,
            columns=columns,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
        )
        portfolio = oporexam.get_DataFramePortfolio_example1(
            event_loop,
            market_data=market_data,
            asset_ids=asset_ids,
        )
        return portfolio

    @staticmethod
    def get_process_forecasts_config() -> cconfig.Config:
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "pomo",
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    @pytest.mark.slow("~8 seconds")
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        start_datetime = pd.Timestamp(
            "2000-01-01 15:00:00-05:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp(
            "2000-01-02 10:30:00-05:00", tz="America/New_York"
        )
        asset_ids = [100, 200]
        # Generate returns predictions and volatility forecasts.
        forecasts = cfidaexa.get_forecast_dataframe(
            start_datetime + pd.Timedelta("5T"),
            end_datetime,
            asset_ids,
        )
        predictions = forecasts["prediction"]
        volatility = forecasts["volatility"]
        # Get portfolio.
        portfolio = self.get_portfolio(
            event_loop, start_datetime, end_datetime, asset_ids
        )
        config = self.get_process_forecasts_config()
        spread_df = None
        restrictions_df = None
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
            spread_df,
            restrictions_df,
        )
        actual = str(portfolio)
        expected = r"""
# historical holdings=
asset_id                     100    200      -1
2000-01-01 15:05:01-05:00   0.00   0.00  1.00e+06
2000-01-01 15:10:01-05:00  23.16 -76.81  1.05e+06
2000-01-01 15:15:01-05:00 -17.16  82.82  9.34e+05
2000-01-01 15:20:01-05:00  53.25 -46.75  9.93e+05
2000-01-01 15:25:01-05:00  55.94 -44.15  9.88e+05
2000-01-01 15:30:01-05:00  56.63 -43.52  9.87e+05
2000-01-01 15:35:01-05:00  59.18 -41.00  9.82e+05
2000-01-01 15:40:01-05:00 -49.39  50.76  9.99e+05
2000-01-01 15:45:01-05:00 -76.18  24.00  1.05e+06
2000-01-01 15:50:01-05:00  79.01 -21.23  9.42e+05
2000-01-01 15:55:01-05:00 -77.16  23.11  1.05e+06
2000-01-02 09:35:01-05:00  69.61 -30.50  9.61e+05
2000-01-02 09:40:01-05:00  69.29 -31.26  9.62e+05
2000-01-02 09:45:01-05:00 -75.77  24.84  1.05e+06
2000-01-02 09:50:01-05:00 -61.76  38.68  1.02e+06
2000-01-02 09:55:01-05:00  51.01 -49.51  9.98e+05
2000-01-02 10:00:01-05:00  65.94 -34.61  9.69e+05
2000-01-02 10:05:01-05:00 -60.05  40.51  1.02e+06
2000-01-02 10:10:01-05:00  60.20 -40.33  9.80e+05
2000-01-02 10:15:01-05:00  59.50 -40.88  9.81e+05
2000-01-02 10:20:01-05:00 -56.53  43.74  1.01e+06
2000-01-02 10:25:01-05:00  49.84 -50.42  1.00e+06
2000-01-02 10:30:01-05:00  50.36 -49.92  9.99e+05
# historical holdings marked to market=
asset_id                        100       200      -1
2000-01-01 15:05:01-05:00      0.00      0.00  1.00e+06
2000-01-01 15:10:01-05:00  23165.76 -76823.19  1.05e+06
2000-01-01 15:15:01-05:00 -17158.23  82813.49  9.34e+05
2000-01-01 15:20:01-05:00  53209.13 -46714.42  9.93e+05
2000-01-01 15:25:01-05:00  55854.91 -44081.63  9.88e+05
2000-01-01 15:30:01-05:00  56527.06 -43436.68  9.87e+05
2000-01-01 15:35:01-05:00  59086.06 -40939.76  9.82e+05
2000-01-01 15:40:01-05:00 -49308.52  50674.96  9.99e+05
2000-01-01 15:45:01-05:00 -75990.51  23941.34  1.05e+06
2000-01-01 15:50:01-05:00  78795.19 -21177.33  9.42e+05
2000-01-01 15:55:01-05:00 -77079.86  23088.29  1.05e+06
2000-01-02 09:35:01-05:00  69224.07 -30327.68  9.61e+05
2000-01-02 09:40:01-05:00  68869.73 -31074.38  9.62e+05
2000-01-02 09:45:01-05:00 -75437.99  24730.16  1.05e+06
2000-01-02 09:50:01-05:00 -61436.86  38480.65  1.02e+06
2000-01-02 09:55:01-05:00  50732.08 -49238.14  9.98e+05
2000-01-02 10:00:01-05:00  65579.16 -34420.39  9.69e+05
2000-01-02 10:05:01-05:00 -59728.49  40289.94  1.02e+06
2000-01-02 10:10:01-05:00  59976.64 -40179.43  9.80e+05
2000-01-02 10:15:01-05:00  59340.24 -40764.31  9.81e+05
2000-01-02 10:20:01-05:00 -56387.48  43631.90  1.01e+06
2000-01-02 10:25:01-05:00  49697.83 -50271.50  1.00e+06
2000-01-02 10:30:01-05:00  50218.09 -49777.00  9.99e+05
# historical flows=
asset_id                         100        200
2000-01-01 15:10:01-05:00  -23159.15   76801.28
2000-01-01 15:15:01-05:00   40325.51 -159646.90
2000-01-01 15:20:01-05:00  -70370.94  129495.31
2000-01-01 15:25:01-05:00   -2680.27   -2603.85
2000-01-01 15:30:01-05:00    -692.65    -629.19
2000-01-01 15:35:01-05:00   -2543.38   -2507.13
2000-01-01 15:40:01-05:00  108399.73  -91620.56
2000-01-01 15:45:01-05:00   26719.20   26702.69
2000-01-01 15:50:01-05:00 -154804.19   45123.57
2000-01-01 15:55:01-05:00  155922.27  -44277.01
2000-01-02 09:35:01-05:00 -146570.63   53536.09
2000-01-02 09:40:01-05:00     315.77     763.94
2000-01-02 09:45:01-05:00  144344.33  -55826.16
2000-01-02 09:50:01-05:00  -13944.34  -13776.26
2000-01-02 09:55:01-05:00 -112172.04   87724.07
2000-01-02 10:00:01-05:00  -14845.36  -14815.59
2000-01-02 10:05:01-05:00  125333.11  -74724.64
2000-01-02 10:10:01-05:00 -119722.94   80481.56
2000-01-02 10:15:01-05:00     698.86     542.68
2000-01-02 10:20:01-05:00  115747.28  -84409.99
2000-01-02 10:25:01-05:00 -106096.25   93915.02
2000-01-02 10:30:01-05:00    -522.91    -492.23
# historical pnl=
asset_id                      100     200
2000-01-01 15:05:01-05:00     NaN     NaN
2000-01-01 15:10:01-05:00    6.61  -21.91
2000-01-01 15:15:01-05:00    1.52  -10.23
2000-01-01 15:20:01-05:00   -3.58  -32.59
2000-01-01 15:25:01-05:00  -34.49   28.94
2000-01-01 15:30:01-05:00  -20.50   15.76
2000-01-01 15:35:01-05:00   15.61  -10.20
2000-01-01 15:40:01-05:00    5.15   -5.84
2000-01-01 15:45:01-05:00   37.21  -30.93
2000-01-01 15:50:01-05:00  -18.49    4.90
2000-01-01 15:55:01-05:00   47.22  -11.39
2000-01-02 09:35:01-05:00 -266.70  120.12
2000-01-02 09:40:01-05:00  -38.57   17.23
2000-01-02 09:45:01-05:00   36.62  -21.63
2000-01-02 09:50:01-05:00   56.79  -25.77
2000-01-02 09:55:01-05:00   -3.10    5.27
2000-01-02 10:00:01-05:00    1.72    2.17
2000-01-02 10:05:01-05:00   25.46  -14.32
2000-01-02 10:10:01-05:00  -17.81   12.19
2000-01-02 10:15:01-05:00   62.46  -42.20
2000-01-02 10:20:01-05:00   19.56  -13.78
2000-01-02 10:25:01-05:00  -10.94   11.61
2000-01-02 10:30:01-05:00   -2.65    2.27
# historical statistics=
                              pnl  gross_volume  net_volume        gmv       nmv      cash  net_wealth  leverage
2000-01-01 15:05:01-05:00     NaN          0.00        0.00       0.00      0.00  1.00e+06  1000000.00       0.0
2000-01-01 15:10:01-05:00  -15.30      99960.44   -53642.13   99988.95 -53657.43  1.05e+06   999984.70       0.1
2000-01-01 15:15:01-05:00   -8.71     199972.41   119321.39   99971.72  65655.25  9.34e+05   999975.99       0.1
2000-01-01 15:20:01-05:00  -36.17     199866.26   -59124.37   99923.55   6494.72  9.93e+05   999939.83       0.1
2000-01-01 15:25:01-05:00   -5.55       5284.11     5284.11   99936.54  11773.28  9.88e+05   999934.27       0.1
2000-01-01 15:30:01-05:00   -4.75       1321.84     1321.84   99963.74  13090.37  9.87e+05   999929.53       0.1
2000-01-01 15:35:01-05:00    5.41       5050.51     5050.51  100025.81  18146.30  9.82e+05   999934.94       0.1
2000-01-01 15:40:01-05:00   -0.69     200020.28   -16779.17   99983.48   1366.44  9.99e+05   999934.25       0.1
2000-01-01 15:45:01-05:00    6.28      53421.88   -53421.88   99931.85 -52049.17  1.05e+06   999940.53       0.1
2000-01-01 15:50:01-05:00  -13.59     199927.76   109680.62   99972.52  57617.86  9.42e+05   999926.93       0.1
2000-01-01 15:55:01-05:00   35.82     200199.28  -111645.26  100168.15 -53991.57  1.05e+06   999962.76       0.1
2000-01-02 09:35:01-05:00 -146.58     200106.72    93034.54   99551.74  38896.39  9.61e+05   999816.18       0.1
2000-01-02 09:40:01-05:00  -21.34       1079.71    -1079.71   99944.11  37795.34  9.62e+05   999794.84       0.1
2000-01-02 09:45:01-05:00   14.99     200170.50   -88518.17  100168.15 -50707.84  1.05e+06   999809.83       0.1
2000-01-02 09:50:01-05:00   31.03      27720.60    27720.60   99917.51 -22956.21  1.02e+06   999840.85       0.1
2000-01-02 09:55:01-05:00    2.17     199896.10    24447.97   99970.22   1493.94  9.98e+05   999843.03       0.1
2000-01-02 10:00:01-05:00    3.89      29660.94    29660.94   99999.55  31158.77  9.69e+05   999846.92       0.1
2000-01-02 10:05:01-05:00   11.14     200057.75   -50608.47  100018.43 -19438.56  1.02e+06   999858.06       0.1
2000-01-02 10:10:01-05:00   -5.62     200204.50    39241.38  100156.07  19797.20  9.80e+05   999852.44       0.1
2000-01-02 10:15:01-05:00   20.26       1241.54    -1241.54  100104.55  18575.93  9.81e+05   999872.70       0.1
2000-01-02 10:20:01-05:00    5.78     200157.27   -31337.30  100019.38 -12755.58  1.01e+06   999878.48       0.1
2000-01-02 10:25:01-05:00    0.67     200011.27    12181.23   99969.33   -573.68  1.00e+06   999879.15       0.1
2000-01-02 10:30:01-05:00   -0.38       1015.14     1015.14   99995.09    441.09  9.99e+05   999878.78       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestSimulatedProcessForecasts3(hunitest.TestCase):
    @staticmethod
    def get_portfolio(
        event_loop, asset_ids: List[int]
    ) -> omportfo.DataFramePortfolio:
        start_datetime = pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        )
        end_datetime = pd.Timestamp(
            "2000-01-01 09:50:00-05:00", tz="America/New_York"
        )
        market_data, _ = mdata.get_ReplayedTimeMarketData_example5(
            event_loop,
            start_datetime,
            end_datetime,
            asset_ids,
        )
        mark_to_market_col = "midpoint"
        portfolio = oporexam.get_DataFramePortfolio_example1(
            event_loop,
            market_data=market_data,
            mark_to_market_col=mark_to_market_col,
            asset_ids=asset_ids,
        )
        return portfolio

    @staticmethod
    def get_process_forecasts_config() -> cconfig.Config:
        dict_ = {
            "order_config": {
                "order_type": "partial_spread_0.25@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "pomo",
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        asset_ids = [101, 202]
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, asset_ids)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, asset_ids)
        # Build a Portfolio.
        portfolio = self.get_portfolio(event_loop, asset_ids)
        # Get process forecasts config.
        config = self.get_process_forecasts_config()
        spread_df = None
        restrictions_df = None
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
            spread_df,
            restrictions_df,
        )
        actual = str(portfolio)
        expected = r"""
# historical holdings=
asset_id                     101    202      -1
2000-01-01 09:35:01-05:00   0.00   0.00  1.00e+06
2000-01-01 09:40:01-05:00 -50.13  49.91  1.00e+06
2000-01-01 09:45:01-05:00 -50.04  49.96  1.00e+06
# historical holdings marked to market=
asset_id                        101       202      -1
2000-01-01 09:35:01-05:00      0.00      0.00  1.00e+06
2000-01-01 09:40:01-05:00 -50091.23  49944.85  1.00e+06
2000-01-01 09:45:01-05:00 -49854.64  50080.69  1.00e+06
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00  50070.28 -49991.02
2000-01-01 09:45:01-05:00    -91.07    -55.25
# historical pnl=
asset_id                      101    202
2000-01-01 09:35:01-05:00     NaN    NaN
2000-01-01 09:40:01-05:00  -20.95 -46.17
2000-01-01 09:45:01-05:00  145.52  80.60
# historical statistics=
                              pnl  gross_volume  net_volume        gmv     nmv      cash  net_wealth  leverage
2000-01-01 09:35:01-05:00     NaN          0.00        0.00       0.00    0.00  1.00e+06    1.00e+06       0.0
2000-01-01 09:40:01-05:00  -67.12     100061.30      -79.26  100036.08 -146.38  1.00e+06    1.00e+06       0.1
2000-01-01 09:45:01-05:00  226.12        146.31      146.31   99935.34  226.05  1.00e+06    1.00e+06       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts1(omtodh.TestOmsDbHelper):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            asset_id_name = "asset_id"
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            #
            oomsdb.create_oms_tables(self.connection, False, asset_id_name)
            #
            portfolio = oporexam.get_DatabasePortfolio_example1(
                event_loop,
                db_connection,
                table_name,
                asset_ids=[101, 202],
            )
            # Build OrderProcessor.
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            termination_condition = 3
            asset_id_name = "asset_id"
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                asset_id_name
            )
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "pomo",
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        spread_df = None
        restrictions_df = None
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
            spread_df,
            restrictions_df,
        )
        # TODO(Paul): Factor out a test that compares simulation and mock.
        actual = str(portfolio)
        expected = r"""
# historical holdings=
asset_id                     101    202       -1
2000-01-01 09:35:01-05:00   0.00   0.00  1000000.0
2000-01-01 09:40:01-05:00 -49.98  49.98  1000000.0
2000-01-01 09:45:01-05:00 -49.99  49.99  1000000.0
# historical holdings marked to market=
asset_id                        101       202       -1
2000-01-01 09:35:01-05:00      0.00      0.00  1000000.0
2000-01-01 09:40:01-05:00 -49994.47  49994.47  1000000.0
2000-01-01 09:45:01-05:00 -49985.86  49985.86  1000000.0
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00  49980.22 -49980.22
2000-01-01 09:45:01-05:00      5.53     -5.53
# historical pnl=
asset_id                     101    202
2000-01-01 09:35:01-05:00    NaN    NaN
2000-01-01 09:40:01-05:00 -14.26  14.26
2000-01-01 09:45:01-05:00  14.14 -14.14
# historical statistics=
                           pnl  gross_volume  net_volume       gmv  nmv       cash  net_wealth  leverage
2000-01-01 09:35:01-05:00  NaN          0.00         0.0      0.00  0.0  1000000.0   1000000.0       0.0
2000-01-01 09:40:01-05:00  0.0      99960.44         0.0  99988.95  0.0  1000000.0   1000000.0       0.1
2000-01-01 09:45:01-05:00  0.0         11.05         0.0  99971.72  0.0  1000000.0   1000000.0       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts2(omtodh.TestOmsDbHelper):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def test_mocked_system1(self) -> None:
        data = self._get_market_data_df1()
        predictions, volatility = self._get_predictions_and_volatility1(data)
        self._run_coroutines(data, predictions, volatility)

    def test_mocked_system2(self) -> None:
        data = self._get_market_data_df2()
        predictions, volatility = self._get_predictions_and_volatility1(data)
        self._run_coroutines(data, predictions, volatility)

    def test_mocked_system3(self) -> None:
        data = self._get_market_data_df1()
        predictions, volatility = self._get_predictions_and_volatility2(data)
        self._run_coroutines(data, predictions, volatility)

    @pytest.mark.skip(
        "This test times out because nothing interesting happens after the first set of orders."
    )
    def test_mocked_system4(self) -> None:
        data = self._get_market_data_df2()
        predictions, volatility = self._get_predictions_and_volatility2(data)
        self._run_coroutines(data, predictions, volatility)

    # TODO(gp): Move to core/finance/market_data_example.py or reuse some of those functions.
    @staticmethod
    def _get_market_data_df1() -> pd.DataFrame:
        """
        Generate price series that alternates every 5 minutes.
        """
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 09:55:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = cofinanc.build_timestamp_df(idx, bar_duration, bar_delay)
        price_pattern = [101.0] * 5 + [100.0] * 5
        price = price_pattern * 2 + [101.0] * 5
        data["price"] = price
        data["asset_id"] = 101
        return data

    # TODO(gp): Move to core/finance/market_data_example.py or reuse some of those functions.
    @staticmethod
    def _get_market_data_df2() -> pd.DataFrame:
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 09:55:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = cofinanc.build_timestamp_df(idx, bar_duration, bar_delay)
        data["price"] = 100
        data["asset_id"] = 101
        return data

    @staticmethod
    def _get_predictions_and_volatility1(
        market_data_df,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate a signal that alternates every 5 minutes.
        """
        # Build predictions.
        asset_id = market_data_df["asset_id"][0]
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        # Sanity check the index (e.g., in case we update the test).
        hdbg.dassert_is_subset(index, market_data_df["end_datetime"].to_list())
        columns = [asset_id]
        prediction_data = [
            [1],
            [-1],
            [1],
            [-1],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        return predictions, volatility

    @staticmethod
    def _get_predictions_and_volatility2(
        market_data_df,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate a signal that is only long.
        """
        # Build predictions.
        asset_id = market_data_df["asset_id"][0]
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        # Sanity check the index (e.g., in case we update the test).
        hdbg.dassert_is_subset(index, market_data_df["end_datetime"].to_list())
        columns = [asset_id]
        prediction_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        return predictions, volatility

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        list_.append(f"{label}=\n{data_str}")

    def _run_coroutines(self, data, predictions, volatility):
        with hasynci.solipsism_context() as event_loop:
            # Build MarketData.
            initial_replayed_delay = 5
            asset_id_name = "asset_id"
            asset_id = [data[asset_id_name][0]]
            market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
                event_loop,
                initial_replayed_delay,
                data,
                asset_id_col_name=asset_id_name
            )
            # Create a portfolio with one asset (and cash).
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_oms_tables(self.connection, False, asset_id_name)
            portfolio = oporexam.get_DatabasePortfolio_example1(
                event_loop,
                db_connection,
                table_name,
                market_data=market_data,
                asset_ids=asset_id,
            )
            # Build OrderProcessor.
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                asset_id_name,
            )
            # Build order process coroutine.
            termination_condition = 4
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(predictions, volatility, portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        predictions: pd.DataFrame,
        volatility: pd.DataFrame,
        portfolio: omportfo.DatabasePortfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
        Portfolio.
        """
        dict_ = {
            "order_config": {
                "order_type": "price@twap",
                "order_duration": 5,
            },
            "optimizer_config": {
                "backend": "pomo",
                "bulk_frac_to_remove": 0.0,
                "bulk_fill_method": "zero",
                "target_gmv": 1e5,
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        spread_df = None
        restrictions_df = None
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
            spread_df,
            restrictions_df,
        )
        #
        asset_ids = portfolio.universe
        hdbg.dassert_eq(len(asset_ids), 1)
        asset_id = asset_ids[0]
        price = portfolio.market_data.get_data_for_interval(
            pd.Timestamp("2000-01-01 09:30:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
            ts_col_name="timestamp_db",
            asset_ids=asset_ids,
            left_close=True,
            right_close=True,
        )["price"]
        #
        twap = cofinanc.resample(price, rule="5T").mean().rename("twap")
        rets = twap.pct_change().rename("rets")
        predictions_srs = predictions[asset_id].rename("prediction")
        research_pnl = (
            predictions_srs.shift(2).multiply(rets).rename("research_pnl")
        )
        #
        actual = []
        self._append(actual, "TWAP", twap)
        self._append(actual, "rets", rets)
        self._append(actual, "prediction", predictions_srs)
        self._append(actual, "research_pnl", research_pnl)
        actual.append(portfolio)
        actual = "\n".join(map(str, actual))
        self.check_string(actual)
