import logging

import numpy as np
import pandas as pd

import core.dataflow_model.pnl_simulator as pnlsim
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPnlSimulatorFunctions1(hut.TestCase):

    def test_get_data1(self) -> None:
        """
        Freeze the output of `_get_data()` as reference for other unit tests.
        """
        df = self._get_data()
        df = df["price ask bid".split()]
        actual_result = hut.convert_df_to_string(df, index=True)
        expected_result = """
                                  price         ask         bid
        2021-09-12 09:30:00  100.496714  100.722490  100.381066
        2021-09-12 09:31:00  100.358450  100.425978  100.057346
        2021-09-12 09:32:00  101.006138  102.430887   99.527616
        2021-09-12 09:33:00  102.529168  103.073551  101.809324
        2021-09-12 09:34:00  102.295015  102.405937  101.834376
        2021-09-12 09:35:00  102.060878  103.211871  101.003756
        2021-09-12 09:36:00  103.640091  104.015789  103.296472
        2021-09-12 09:37:00  104.407525  105.008164  102.644485
        2021-09-12 09:38:00  103.938051  104.229745  103.613967
        2021-09-12 09:39:00  104.480611  105.082318  104.095529
        2021-09-12 09:40:00  104.017193  105.869472  103.340271
        2021-09-12 09:41:00  103.551464  103.564961  102.939787
        2021-09-12 09:42:00  103.793426  104.851137  102.762426
        2021-09-12 09:43:00  101.880146  102.702691  100.948866
        2021-09-12 09:44:00  100.155228  101.376072   99.316010
        2021-09-12 09:45:00   99.592940   99.801804   99.283728
        2021-09-12 09:46:00   98.580109  100.539779   98.248846
        2021-09-12 09:47:00   98.894357  100.222543   97.918811
        2021-09-12 09:48:00   97.986332   98.183194   97.507158
        2021-09-12 09:49:00   96.574029   97.312495   96.388370
        2021-09-12 09:50:00   98.039678   98.211046   96.933343
        """
        expected_result = hprint.dedent(expected_result)
        self.assert_equal(actual_result, expected_result)

    def test_get_twap_price1(self) -> None:
        """
        Test that TWAP is computed properly.
        """
        df = self._get_data()
        ts_start = pd.Timestamp("2021-09-12 09:30:00")
        ts_end = pd.Timestamp("2021-09-12 09:35:00")
        column = "price"
        act = pnlsim.get_twap_price(df, ts_start, ts_end, column)
        #
        exp = df.loc[ts_start + pd.Timedelta(minutes=1) : ts_end]["price"].mean()
        np.testing.assert_almost_equal(act, exp)
        #
        exp = (
            100.358450 + 101.006138 + 102.529168 + 102.295015 + 102.060878
        ) / 5.0
        np.testing.assert_almost_equal(act, exp)

    def _get_data(self) -> pd.DataFrame:
        """
        Return fixed random data for the other unit tests.
        """
        num_samples = 21
        seed = 42
        df = pnlsim.get_random_market_data(num_samples, seed)
        return df


class TestPnlSimulator1(hut.TestCase):
    """
    Verify that computing PnL using `compute_pnl_level1()`, `compute_lag_pnl()`
    and `compute_pnl_level2()` yield the same results.
    """

    def test1(self) -> None:
        """
        Compute PnL on an handcrafted example.
        """
        df, df_5mins = pnlsim.get_example_market_data1()
        # Execute.
        self._run(df, df_5mins)

    def test_random1(self) -> None:
        """
        Compute PnL on a random example.
        """
        num_samples = 5 * 3 + 1
        seed = 42
        df, df_5mins = pnlsim.get_example_market_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def test_random2(self) -> None:
        """
        Compute PnL on a random example.
        """
        num_samples = 5 * 10 + 1
        seed = 43
        df, df_5mins = pnlsim.get_example_market_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def test_random3(self) -> None:
        """
        Compute PnL on a random example.
        """
        num_samples = 5 * 20 + 1
        seed = 44
        df, df_5mins = pnlsim.get_example_market_data2(num_samples, seed)
        # Execute.
        self._run(df, df_5mins)

    def _run(self, df: pd.DataFrame, df_5mins: pd.DataFrame) -> None:
        """
        Compute PnL using lag-based approach, level1 simulation, and level2
        simulations, checking that:

        - the intermediate PnL stream match
        - the total return from the different approaches matches
        """
        act = []
        act.append("df=\n%s" % hut.convert_df_to_string(df, index=True))
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using simulation level 1.
        initial_wealth = 1000.0
        (
            final_w,
            tot_ret,
            df_5mins,
        ) = pnlsim.compute_pnl_level1(initial_wealth, df, df_5mins)
        act.append("# tot_ret=%s" % tot_ret)
        act.append(
            "After pnl simulation level 1: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        # Compute pnl using lags.
        tot_ret_lag, df_5mins = pnlsim.compute_lag_pnl(df_5mins)
        act.append(
            "After pnl lag computation: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        act.append("# tot_ret_lag=%s" % tot_ret_lag)
        # Compute pnl using simulation level 2.
        config = {
            "price_column": "price",
            "future_snoop_allocation": True,
            "order_type": "price.end",
        }
        df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)
        act.append(
            "After pnl simulation level 2: df_5mins=\n%s"
            % hut.convert_df_to_string(df_5mins, index=True)
        )
        #
        act = "\n".join(act)
        self.check_string(act)
        # Check that all the realized PnL are the same.
        df_5mins["pnl.sim2.shifted(-2)"] = df_5mins["pnl.sim2"].shift(-2)
        for col in ["pnl.lag", "pnl.sim1", "pnl.sim2.shifted(-2)"]:
            df_5mins[col] = df_5mins[col].replace(np.nan, 0)
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim1"]
        )
        np.testing.assert_array_almost_equal(
            df_5mins["pnl.lag"], df_5mins["pnl.sim2.shifted(-2)"]
        )
        # Check that the total returns are the same.
        np.testing.assert_almost_equal(tot_ret, tot_ret_lag)


class TestPnlSimulator2(hut.TestCase):
    def test1(self) -> None:
        """
        Run level2 simulation using future information to use invest all the
        working capital.
        """
        act = []
        #
        df, df_5mins = pnlsim.get_example_market_data1()
        initial_wealth = 1000.0
        #
        config = {
            "price_column": "price",
            "future_snoop_allocation": True,
            "order_type": "price.end",
        }
        df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        #
        act = "\n".join(act)
        self.check_string(act)

    def test2(self) -> None:
        """
        Same as `test1()` but without future information.
        """
        act = []
        #
        df, df_5mins = pnlsim.get_example_market_data1()
        initial_wealth = 1000.0
        #
        config = {
            "price_column": "price",
            "future_snoop_allocation": False,
            "order_type": "price.end",
        }
        df_5mins = pnlsim.compute_pnl_level2(df, df_5mins, initial_wealth, config)
        act.append(
            "df_5mins=\n%s" % hut.convert_df_to_string(df_5mins, index=True)
        )
        #
        act = "\n".join(act)
        self.check_string(act)


# TODO(gp): Add unit tests for computing PnL with level2 sim using midpoint price,
#  and different spread amount.
