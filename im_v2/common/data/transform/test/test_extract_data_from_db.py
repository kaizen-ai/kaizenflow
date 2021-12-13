import os

import pytest

import helpers.git as hgit
import helpers.io_ as hio
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im.ccxt.db.utils as imccdbuti
import im_v2.common.db.utils as imcodbuti


class TestExtractDataFromDb1(imcodbuti.TestImDbHelper):
    def setUp(self) -> None:
        super().setUp()
        # TODO(Nikola): linter is complaining about cursor and create database?
        ccxt_ohlcv_table_query = imccdbuti.get_ccxt_ohlcv_create_table_query()
        ccxt_ohlcv_insert_query = """
        INSERT INTO ccxt_ohlcv
        VALUES
            (66, 1637690340000, 1.04549, 1.04549, 1.04527, 1.04527,
            5898.0427797325265, 'XRP_USDT', 'gateio', '2021-11-23 18:03:54.318763'),
            (71, 1637777340000, 221.391, 221.493, 221.297, 221.431,
            81.31775837, 'SOL_USDT', 'kucoin', '2021-11-23 18:03:54.676947')
        """
        with self.connection.cursor() as cursor:
            cursor.execute(ccxt_ohlcv_table_query)
            cursor.execute(ccxt_ohlcv_insert_query)

    def tearDown(self) -> None:
        super().tearDown()
        ccxt_ohlcv_drop_query = """
        DROP TABLE IF EXISTS ccxt_ohlcv;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(ccxt_ohlcv_drop_query)

    @pytest.mark.slow
    def test_extract_data_from_db(self) -> None:
        test_dir = self.get_scratch_space()
        dst_dir = os.path.join(test_dir, "by_date")
        hio.create_dir(dst_dir, False)

        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/extract_data_from_db.py",
        )
        cmd = []
        cmd.append(file_path)
        cmd.append("--start_date 2021-11-23")
        cmd.append("--end_date 2021-11-25")
        cmd.append(f"--dst_dir {dst_dir}")
        cmd = " ".join(cmd)
        include_file_content = True
        daily_signature_before = hunitest.get_dir_signature(
            dst_dir, include_file_content
        )
        hsysinte.system(cmd)
        # Check directory structure with file contents.
        act = []
        act.append("# before=")
        act.append(daily_signature_before)
        daily_signature_after = hunitest.get_dir_signature(
            dst_dir, include_file_content
        )
        act.append("# after=")
        act.append(daily_signature_after)
        act = "\n".join(act)
        self.check_string(act)
