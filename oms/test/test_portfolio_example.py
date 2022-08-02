import helpers.hobject as hobject
import helpers.hunit_test as hunitest


# #############################################################################
# Test_Portfolio_builders1
# #############################################################################


class Test_Portfolio_builders1(hunitest.TestCase):

    def test1(self) -> None:
        event_loop = None
        portfolio = get_DataFramePortfolio_example1(event_loop)
        #
        hobject.test_object_signature(portfolio)