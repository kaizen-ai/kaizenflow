import pandas as pd

import helpers.unit_test as hut
import vendors2.sp_constituents.universe as sp_uv


def _test_over_time(sp):
    """
    Test SP_Universe instance over range of dates to contain expected number of tickers.

    :param sp: instance of SP_Universe class
    :return: tsv string with date and count columns
    """
    date_range = pd.date_range(
        start="2017-01-01", end="2020-01-01", freq="5D"
    )
    results = pd.DataFrame(columns=["date", "count"], dtype=str)
    for curr_date in date_range:
        # Check number of constituents for each day.
        curr_date = curr_date.strftime("%Y-%m-%d")
        results.loc[len(results)] = [
            curr_date,
            len(sp.get_constituents(curr_date)),
        ]
    return hut.convert_df_to_string(results)


def _test_as_of_date(sp):
    """
    Test SP_universe instance's ability to produce a valid list of tickers.

    :param sp: instance of SP_Universe class
    :return: tsv string with ticker and date columns
    """
    return hut.convert_df_to_string(sp.get_constituents("2017-01-01"))


class TestGetConstituents(hut.TestCase):

    def test_sp400_as_of_date(self) -> None:
        """
        Test that S&P 400 constituents list is correctly parsed.
        """
        sp400 = sp_uv.get_sp400_universe()
        actual = _test_as_of_date(sp400)
        # Check result.
        self.check_string(actual)

    def test_sp400_over_time(self) -> None:
        """
        Test that S&P 400 oscillates around 400 over time.
        """
        sp400 = sp_uv.get_sp400_universe()
        actual = _test_over_time(sp400)
        # Check result.
        self.check_string(actual)

    def test_sp500_as_of_date(self) -> None:
        """
        Test that S&P 500 constituents list is correctly parsed.
        """
        sp500 = sp_uv.get_sp500_universe()
        actual = _test_as_of_date(sp500)
        # Check result.
        self.check_string(actual)

    def test_sp500_over_time(self) -> None:
        """
        Test that S&P 500 oscillates around 505 over time.
        """
        sp500 = sp_uv.get_sp500_universe()
        actual = _test_over_time(sp500)
        # Check result.
        self.check_string(actual)

    def test_sp600_as_of_date(self) -> None:
        """
        Test that S&P 600 constituents list is correctly parsed.
        """
        sp600 = sp_uv.get_sp600_universe()
        actual = _test_as_of_date(sp600)
        # Check result.
        self.check_string(actual)

    def test_sp600_over_time(self) -> None:
        """
        Test that S&P 600 oscillates around 983 over time.
        """
        sp600 = sp_uv.get_sp600_universe()
        actual = _test_over_time(sp600)
        # Check result.
        self.check_string(actual)
