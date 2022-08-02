import helpers.hobject as hobject
import helpers.hunit_test as hunitest
import oms.portfolio_example as oporexam


# #############################################################################
# Test_Portfolio_builders1
# #############################################################################


class Test_Portfolio_builders1(hunitest.TestCase):

    def test1(self) -> None:
        event_loop = None
        portfolio = oporexam.get_DataFramePortfolio_example1(event_loop)
        #
        hobject.test_object_signature(self, portfolio)