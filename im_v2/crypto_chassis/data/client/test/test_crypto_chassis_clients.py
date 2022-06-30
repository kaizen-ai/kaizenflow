import os

import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce


class TestCryptoChassisHistoricalPqByTileClient1(hunitest.TestCase):
    def test1(self) -> None:
        """
        `dataset = bid_ask-futures`
        """
        resample1_min = True
        client = imvccdcccce.get_CryptoChassisHistoricalPqByTileClient_example4(
            resample1_min
        )




