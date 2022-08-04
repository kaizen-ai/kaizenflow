from typing import Any

import helpers.hasyncio as hasynci
import helpers.hobject as hobject
import helpers.hunit_test as hunitest
import oms.oms_db as oomsdb
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as omtodh


def _test_object_signature(self_: Any, obj: Any) -> None:
    remove_lines_regex = "_db_connection"
    hobject.test_object_signature(
        self_, obj, remove_lines_regex=remove_lines_regex
    )


# #############################################################################
# Test_Portfolio_builders1
# #############################################################################


class Test_Portfolio_builders1(hunitest.TestCase):
    def test1(self) -> None:
        event_loop = None
        portfolio = oporexam.get_DataFramePortfolio_example1(event_loop)
        #
        _test_object_signature(self, portfolio)


# #############################################################################
# Test_Portfolio_builders2
# #############################################################################


class Test_Portfolio_builders2(omtodh.TestOmsDbHelper):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def test1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            # Create DatabasePortfolio with some initial cash.
            portfolio = oporexam.get_DatabasePortfolio_example1(
                event_loop,
                self.connection,
                table_name,
                asset_ids=[101],
            )
            _test_object_signature(self, portfolio)
