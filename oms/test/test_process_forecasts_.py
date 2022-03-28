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

_LOG = logging.getLogger(__name__)


class TestSimulatedProcessForecasts1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

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
                "backend": "compute_target_positions_in_cash",
                "target_gmv": 1e5,
                "dollar_neutrality": "no_constraint",
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

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
asset_id                     101    202        -1
2000-01-01 09:35:01-05:00   0.00   0.00  1000000.00
2000-01-01 09:40:01-05:00  33.32  66.65   900039.56
2000-01-01 09:45:01-05:00 -24.99  74.98   950024.38
# historical holdings marked to market=
asset_id                        101       202        -1
2000-01-01 09:35:01-05:00      0.00      0.00  1000000.00
2000-01-01 09:40:01-05:00  33329.65  66659.30   900039.56
2000-01-01 09:45:01-05:00 -24992.93  74978.79   950024.38
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00 -33320.15 -66640.29
2000-01-01 09:45:01-05:00  58324.83  -8340.01
# historical pnl=
asset_id                    101    202
2000-01-01 09:35:01-05:00   NaN    NaN
2000-01-01 09:40:01-05:00  9.50  19.01
2000-01-01 09:45:01-05:00  2.25 -20.52
# historical statistics=
                             pnl  gross_volume  net_volume       gmv       nmv        cash  net_wealth  leverage
2000-01-01 09:35:01-05:00    NaN          0.00        0.00      0.00      0.00  1000000.00    1.00e+06       0.0
2000-01-01 09:40:01-05:00  28.51      99960.44    99960.44  99988.95  99988.95   900039.56    1.00e+06       0.1
2000-01-01 09:45:01-05:00 -18.28      66664.84   -49984.81  99971.72  49985.86   950024.38    1.00e+06       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestSimulatedProcessForecasts2(hunitest.TestCase):
    @pytest.mark.slow("~8 seconds")
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

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
                "backend": "compute_target_positions_in_cash",
                "target_gmv": 1e5,
                "dollar_neutrality": "no_constraint",
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

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
2000-01-01 15:10:01-05:00 -17.14 -82.83  1.10e+06
2000-01-01 15:15:01-05:00 -59.87  40.11  1.02e+06
2000-01-01 15:20:01-05:00 -51.29 -48.72  1.10e+06
2000-01-01 15:25:01-05:00  44.89 -55.19  1.01e+06
2000-01-01 15:30:01-05:00  94.46  -5.69  9.11e+05
2000-01-01 15:35:01-05:00  45.65 -54.54  1.01e+06
2000-01-01 15:40:01-05:00 -41.99  58.17  9.84e+05
2000-01-01 15:45:01-05:00 -86.83 -13.35  1.10e+06
2000-01-01 15:50:01-05:00  63.51 -36.73  9.73e+05
2000-01-01 15:55:01-05:00 -38.34  61.93  9.76e+05
2000-01-02 09:35:01-05:00  48.22 -51.88  1.00e+06
2000-01-02 09:40:01-05:00  21.48 -79.07  1.06e+06
2000-01-02 09:45:01-05:00  22.03  78.58  9.00e+05
2000-01-02 09:50:01-05:00  41.04  59.40  9.00e+05
2000-01-02 09:55:01-05:00 -14.84 -85.68  1.10e+06
2000-01-02 10:00:01-05:00 -46.82 -53.73  1.10e+06
2000-01-02 10:05:01-05:00 -60.92  39.63  1.02e+06
2000-01-02 10:10:01-05:00  61.76 -38.78  9.77e+05
2000-01-02 10:15:01-05:00  97.94   2.44  9.00e+05
2000-01-02 10:20:01-05:00 -40.72  59.56  9.81e+05
2000-01-02 10:25:01-05:00  90.13  10.13  9.00e+05
2000-01-02 10:30:01-05:00  59.06 -41.22  9.82e+05
# historical holdings marked to market=
asset_id                        100       200      -1
2000-01-01 15:05:01-05:00      0.00      0.00  1.00e+06
2000-01-01 15:10:01-05:00 -17146.70 -82842.25  1.10e+06
2000-01-01 15:15:01-05:00 -59869.47  40102.25  1.02e+06
2000-01-01 15:20:01-05:00 -51243.85 -48679.70  1.10e+06
2000-01-01 15:25:01-05:00  44825.88 -55110.66  1.01e+06
2000-01-01 15:30:01-05:00  94286.68  -5677.06  9.11e+05
2000-01-01 15:35:01-05:00  45577.11 -54448.70  1.01e+06
2000-01-01 15:40:01-05:00 -41918.48  58065.00  9.84e+05
2000-01-01 15:45:01-05:00 -86619.12 -13312.73  1.10e+06
2000-01-01 15:50:01-05:00  63343.22 -36629.30  9.73e+05
2000-01-01 15:55:01-05:00 -38299.33  61868.82  9.76e+05
2000-01-02 09:35:01-05:00  47959.40 -51592.35  1.00e+06
2000-01-02 09:40:01-05:00  21353.10 -78591.01  1.06e+06
2000-01-02 09:45:01-05:00  21937.77  78230.37  9.00e+05
2000-01-02 09:50:01-05:00  40826.72  59090.80  9.00e+05
2000-01-02 09:55:01-05:00 -14758.62 -85211.60  1.10e+06
2000-01-02 10:00:01-05:00 -46561.74 -53437.80  1.10e+06
2000-01-02 10:05:01-05:00 -60599.15  39419.27  1.02e+06
2000-01-02 10:10:01-05:00  61525.22 -38630.85  9.77e+05
2000-01-02 10:15:01-05:00  97671.65   2432.90  9.00e+05
2000-01-02 10:20:01-05:00 -40614.15  59405.23  9.81e+05
2000-01-02 10:25:01-05:00  89870.44  10098.89  9.00e+05
2000-01-02 10:30:01-05:00  58892.88 -41102.20  9.82e+05
# historical flows=
asset_id                         100        200
2000-01-01 15:10:01-05:00   17141.81   82818.62
2000-01-01 15:15:01-05:00   42736.17 -122945.67
2000-01-01 15:20:01-05:00   -8581.89   88772.36
2000-01-01 15:25:01-05:00  -96063.23    6463.60
2000-01-01 15:30:01-05:00  -49494.98  -49431.52
2000-01-01 15:35:01-05:00   48714.29   48750.54
2000-01-01 15:40:01-05:00   87500.10 -112520.19
2000-01-01 15:45:01-05:00   44735.25   71347.78
2000-01-01 15:50:01-05:00 -149976.69   23326.16
2000-01-01 15:55:01-05:00  101693.44  -98505.84
2000-01-02 09:35:01-05:00  -86448.13  113658.58
2000-01-02 09:40:01-05:00   26589.33   27037.50
2000-01-02 09:45:01-05:00    -548.47 -156867.48
2000-01-02 09:50:01-05:00  -18914.41   19082.48
2000-01-02 09:55:01-05:00   55583.78  144312.33
2000-01-02 10:00:01-05:00   31799.02  -31769.24
2000-01-02 10:05:01-05:00   14030.33  -92876.84
2000-01-02 10:10:01-05:00 -122142.02   78062.48
2000-01-02 10:15:01-05:00  -36069.47  -41089.75
2000-01-02 10:20:01-05:00  138314.36  -56975.83
2000-01-02 10:25:01-05:00 -130506.86   49301.24
2000-01-02 10:30:01-05:00   30985.48   51220.99
# historical pnl=
asset_id                      100     200
2000-01-01 15:05:01-05:00     NaN     NaN
2000-01-01 15:10:01-05:00   -4.89  -23.62
2000-01-01 15:15:01-05:00   13.40   -1.18
2000-01-01 15:20:01-05:00   43.73   -9.60
2000-01-01 15:25:01-05:00    6.50   32.64
2000-01-01 15:30:01-05:00  -34.19    2.07
2000-01-01 15:35:01-05:00    4.72  -21.10
2000-01-01 15:40:01-05:00    4.51   -6.48
2000-01-01 15:45:01-05:00   34.60  -29.95
2000-01-01 15:50:01-05:00  -14.34    9.59
2000-01-01 15:55:01-05:00   50.89   -7.72
2000-01-02 09:35:01-05:00 -189.41  197.41
2000-01-02 09:40:01-05:00  -16.97   38.84
2000-01-02 09:45:01-05:00   36.21  -46.09
2000-01-02 09:50:01-05:00  -25.47  -57.09
2000-01-02 09:55:01-05:00   -1.56    9.93
2000-01-02 10:00:01-05:00   -4.11    4.56
2000-01-02 10:05:01-05:00   -7.08  -19.76
2000-01-02 10:10:01-05:00  -17.65   12.36
2000-01-02 10:15:01-05:00   76.97  -25.99
2000-01-02 10:20:01-05:00   28.56   -3.49
2000-01-02 10:25:01-05:00  -22.28   -5.10
2000-01-02 10:30:01-05:00    7.92   19.90
# historical statistics=
                             pnl  gross_volume  net_volume        gmv        nmv      cash  net_wealth  leverage
2000-01-01 15:05:01-05:00    NaN          0.00        0.00       0.00       0.00  1.00e+06    1.00e+06       0.0
2000-01-01 15:10:01-05:00 -28.51      99960.44   -99960.44   99988.95  -99988.95  1.10e+06    1.00e+06       0.1
2000-01-01 15:15:01-05:00  12.23     165681.84    80209.50   99971.72  -19767.22  1.02e+06    1.00e+06       0.1
2000-01-01 15:20:01-05:00  34.14      97354.24   -80190.47   99923.55  -99923.55  1.10e+06    1.00e+06       0.1
2000-01-01 15:25:01-05:00  39.14     102526.83    89599.63   99936.54  -10284.78  1.01e+06    1.00e+06       0.1
2000-01-01 15:30:01-05:00 -32.11      98926.51    98926.51   99963.74   88609.62  9.11e+05    1.00e+06       0.1
2000-01-01 15:35:01-05:00 -16.38      97464.83   -97464.83  100025.81   -8871.59  1.01e+06    1.00e+06       0.1
2000-01-01 15:40:01-05:00  -1.98     200020.28    25020.09   99983.48   16146.52  9.84e+05    1.00e+06       0.1
2000-01-01 15:45:01-05:00   4.66     116083.03  -116083.03   99931.85  -99931.85  1.10e+06    1.00e+06       0.1
2000-01-01 15:50:01-05:00  -4.75     173302.85   126650.53   99972.52   26713.93  9.73e+05    1.00e+06       0.1
2000-01-01 15:55:01-05:00  43.18     200199.28    -3187.61  100168.15   23569.50  9.76e+05    1.00e+06       0.1
2000-01-02 09:35:01-05:00   8.00     200106.72   -27210.45   99551.74   -3632.95  1.00e+06    1.00e+06       0.1
2000-01-02 09:40:01-05:00  21.86      53626.83   -53626.83   99944.11  -57237.91  1.06e+06    1.00e+06       0.1
2000-01-02 09:45:01-05:00  -9.89     157415.95   157415.95  100168.15  100168.15  9.00e+05    1.00e+06       0.1
2000-01-02 09:50:01-05:00 -82.56      37996.89     -168.07   99917.51   99917.51  9.00e+05    1.00e+06       0.1
2000-01-02 09:55:01-05:00   8.37     199896.10  -199896.10   99970.22  -99970.22  1.10e+06    1.00e+06       0.1
2000-01-02 10:00:01-05:00   0.45      63568.26      -29.77   99999.55  -99999.55  1.10e+06    1.00e+06       0.1
2000-01-02 10:05:01-05:00 -26.84     106907.16    78846.51  100018.43  -21179.88  1.02e+06    1.00e+06       0.1
2000-01-02 10:10:01-05:00  -5.29     200204.50    44079.53  100156.07   22894.36  9.77e+05    1.00e+06       0.1
2000-01-02 10:15:01-05:00  50.98      77159.22    77159.22  100104.55  100104.55  9.00e+05    1.00e+06       0.1
2000-01-02 10:20:01-05:00  25.06     195290.19   -81338.53  100019.38   18791.09  9.81e+05    1.00e+06       0.1
2000-01-02 10:25:01-05:00 -27.38     179808.11    81205.62   99969.33   99969.33  9.00e+05    1.00e+06       0.1
2000-01-02 10:30:01-05:00  27.81      82206.47   -82206.47   99995.09   17790.68  9.82e+05    1.00e+06       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestSimulatedProcessForecasts3(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

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
                "backend": "compute_target_positions_in_cash",
                "target_gmv": 1e5,
                "dollar_neutrality": "no_constraint",
            },
            "execution_mode": "batch",
            "ath_start_time": datetime.time(9, 30),
            "trading_start_time": datetime.time(9, 35),
            "ath_end_time": datetime.time(16, 00),
            "trading_end_time": datetime.time(15, 55),
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

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
asset_id                     101    202        -1
2000-01-01 09:35:01-05:00   0.00   0.00  1000000.00
2000-01-01 09:40:01-05:00  33.42  66.55   899964.50
2000-01-01 09:45:01-05:00 -25.02  74.95   949830.54
# historical holdings marked to market=
asset_id                        101       202        -1
2000-01-01 09:35:01-05:00      0.00      0.00  1000000.00
2000-01-01 09:40:01-05:00  33395.11  66592.43   899964.50
2000-01-01 09:45:01-05:00 -24926.68  75122.70   949830.54
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00 -33380.55 -66654.94
2000-01-01 09:45:01-05:00  58288.48  -8422.44
# historical pnl=
asset_id                     101     202
2000-01-01 09:35:01-05:00    NaN     NaN
2000-01-01 09:40:01-05:00  14.56  -62.51
2000-01-01 09:45:01-05:00 -33.31  107.83
# historical statistics=
                             pnl  gross_volume  net_volume        gmv       nmv        cash  net_wealth  leverage
2000-01-01 09:35:01-05:00    NaN          0.00        0.00       0.00      0.00  1000000.00    1.00e+06       0.0
2000-01-01 09:40:01-05:00 -47.96     100035.50   100035.50   99987.54  99987.54   899964.50    1.00e+06       0.1
2000-01-01 09:45:01-05:00  74.52      66710.92   -49866.04  100049.39  50196.02   949830.54    1.00e+06       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts1(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            #
            oomsdb.create_oms_tables(self.connection, incremental=False)
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                asset_ids=[101, 202],
            )
            # Build OrderProcessor.
            get_wall_clock_time = portfolio._get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            # poll_kwargs["sleep_in_secs"] = 1
            poll_kwargs["timeout_in_secs"] = 60 * 10
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            termination_condition = 3
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
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
                "backend": "compute_target_positions_in_cash",
                "target_gmv": 1e5,
                "dollar_neutrality": "no_constraint",
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
asset_id                     101    202        -1
2000-01-01 09:35:01-05:00   0.00   0.00  1000000.00
2000-01-01 09:40:01-05:00  33.32  66.65   900039.56
2000-01-01 09:45:01-05:00 -24.99  74.98   950024.38
# historical holdings marked to market=
asset_id                        101       202        -1
2000-01-01 09:35:01-05:00      0.00      0.00  1000000.00
2000-01-01 09:40:01-05:00  33329.65  66659.30   900039.56
2000-01-01 09:45:01-05:00 -24992.93  74978.79   950024.38
# historical flows=
asset_id                        101       202
2000-01-01 09:40:01-05:00 -33320.15 -66640.29
2000-01-01 09:45:01-05:00  58324.83  -8340.01
# historical pnl=
asset_id                    101    202
2000-01-01 09:35:01-05:00   NaN    NaN
2000-01-01 09:40:01-05:00  9.50  19.01
2000-01-01 09:45:01-05:00  2.25 -20.52
# historical statistics=
                             pnl  gross_volume  net_volume       gmv       nmv        cash  net_wealth  leverage
2000-01-01 09:35:01-05:00    NaN          0.00        0.00      0.00      0.00  1000000.00    1.00e+06       0.0
2000-01-01 09:40:01-05:00  28.51      99960.44    99960.44  99988.95  99988.95   900039.56    1.00e+06       0.1
2000-01-01 09:45:01-05:00 -18.28      66664.84   -49984.81  99971.72  49985.86   950024.38    1.00e+06       0.1"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts2(omtodh.TestOmsDbHelper):
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

    def _run_coroutines(self, data, predictions, volatility):
        with hasynci.solipsism_context() as event_loop:
            # Build MarketData.
            initial_replayed_delay = 5
            asset_id = [data["asset_id"][0]]
            market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
                event_loop,
                initial_replayed_delay,
                data,
            )
            # Create a portfolio with one asset (and cash).
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_oms_tables(self.connection, incremental=False)
            portfolio = oporexam.get_mocked_portfolio_example1(
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
            poll_kwargs = hasynci.get_poll_kwargs(portfolio._get_wall_clock_time)
            poll_kwargs["timeout_in_secs"] = 60 * 10
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
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
        portfolio: omportfo.MockedPortfolio,
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
                "backend": "compute_target_positions_in_cash",
                "target_gmv": 1e5,
                "dollar_neutrality": "no_constraint",
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
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        price_pattern = [101.0] * 5 + [100.0] * 5
        price = price_pattern * 2 + [101.0] * 5
        data["price"] = price
        data["asset_id"] = 101
        return data

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
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
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
