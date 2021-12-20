import os

import helpers.git as hgit
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mclient

_LOCAL_ROOT_DIR = os.path.join(
    hgit.get_client_root(False),
    "im_v2/ccxt/data/client/test/test_data",
)


class TestGetData(hunitest.TestCase):
    def test1(self):
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        multiple_symbols_client = imvcdcli.MultipleSymbolsImClient(ccxt_file_client, "concat")
        