import helpers.unit_test as hunitest

import im_v2.common.data.client.abstract_data_loader as imvcdcadlo


class TestGetFilePath(hunitest.TestCase):
    def test1(self):
        full_symbol = 1
        imvcdcadlo.dassert_correct_full_symbol_format(full_symbol)
