import helpers.hunit_test as hunitest
import im_v2.common.universe.universe_utils as imvcuunut


class TestStringToNumId(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that string id is converted to numerical correctly.
        """
        num_id = imvcuunut.string_to_numerical_id("binance::BTC_USDT")
        self.assertEqual(num_id, 1467591036)


class TestBuildNumericToStringIdMapping(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that numerical to string ids mapping is being built correctly.
        """
<<<<<<< HEAD
        mapping = imvcuunut.build_numerical_to_string_id_mapping(
            ("gateio::XRP_USDT", "kucoin::SOL_USDT")
=======
<<<<<<< HEAD
        mapping = imvcuunut.build_numerical_to_string_id_mapping(
            ("gateio::XRP_USDT", "kucoin::SOL_USDT")
=======
        mapping = imvcuunut.build_num_to_string_id_mapping(
            ["gateio::XRP_USDT", "kucoin::SOL_USDT"]
>>>>>>> 8b50dc45745d85cc64827368c95143fe05d339ae
>>>>>>> 06992d9b2b8c1de0e3c00a1ad3a6fc03e6e66929
        )
        self.assertEqual(len(mapping), 2)
        self.assert_equal(mapping[2002879833], "gateio::XRP_USDT")
        self.assert_equal(mapping[2568064341], "kucoin::SOL_USDT")
