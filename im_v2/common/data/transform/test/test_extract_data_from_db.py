import argparse
import os
from typing import Dict, Generator, List, Tuple

import pytest

import helpers.git as hgit
import helpers.io_ as hio
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.transform.extract_data_from_db as imvcdtedfd
import im_v2.common.db.utils as imvcodbut


class TestExtractDataFromDb1(imvcodbut.TestImDbHelper):
    def setUp(self) -> None:
        super().setUp()
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        ccxt_ohlcv_insert_query = """
        INSERT INTO ccxt_ohlcv
        VALUES
            (66, 1637690340000, 1.04549, 1.04549, 1.04527, 1.04527,
            5898.0427797325265, 'XRP_USDT', 'gateio', '2021-11-23 18:03:54.318763'),
            (71, 1637777340000, 221.391, 221.493, 221.297, 221.431,
            81.31775837, 'SOL_USDT', 'kucoin', '2021-11-23 18:03:54.676947')
        """
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)
        hsql.execute_query(self.connection, ccxt_ohlcv_insert_query)

    def tearDown(self) -> None:
        super().tearDown()
        ccxt_ohlcv_drop_query = """
        DROP TABLE IF EXISTS ccxt_ohlcv;
        """
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def check_directory_structure_with_file_contents(
        self, by_date_dir: str
    ) -> Generator:
        """
        Generate directory and file structure together with file contents in
        form of string. String is compared with previously generated .txt file
        for any differences.

        :param by_date_dir: directory where daily PQ files are saved
        """
        include_file_content = True
        daily_signature_before = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        actual = []
        actual.append("# before=")
        actual.append(daily_signature_before)
        daily_signature_after = hunitest.get_dir_signature(
            by_date_dir, include_file_content
        )
        yield
        actual.append("# after=")
        actual.append(daily_signature_after)
        actual = "\n".join(actual)
        purify_text = True
        self.check_string(actual, purify_text=purify_text)

    # @pytest.mark.slow
    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_extract_data_from_db(self) -> None:
        by_date_dir, file_path = self._prepare_scratch_space()
        cmd = self._prepare_command_args(by_date_dir)
        # Insert file path as first command argument.
        cmd.insert(0, file_path)
        cmd = " ".join(cmd)
        check_gen = self.check_directory_structure_with_file_contents(by_date_dir)
        next(check_gen)
        hsysinte.system(cmd)
        next(check_gen)

    # @pytest.mark.slow
    @pytest.mark.skip("Enable when purify_text issue is resolved CMTask782")
    def test_extract_data_from_db_direct_run(self) -> None:
        by_date_dir, _ = self._prepare_scratch_space()
        cmd = self._prepare_command_args(by_date_dir)
        # Prepare args for kwargs conversion.
        cmd = [c.lstrip("--").split(" ") for c in cmd]
        kwargs: Dict[str, str] = dict(cmd)
        kwargs.update({"stage": "local"})
        args = argparse.Namespace(**kwargs)
        check_gen = self.check_directory_structure_with_file_contents(by_date_dir)
        next(check_gen)
        imvcdtedfd._run(args)
        next(check_gen)

    def test_parser(self) -> None:
        """
        Tests arg parser for predefined args in the script.
        """
        parser = imvcdtedfd._parse()
        by_date_dir = "dummy_by_date_dir"
        cmd = self._prepare_command_args(by_date_dir)
        # Prepare commands for parse_args.
        cmd = [cmd_part for c in cmd for cmd_part in c.split(" ")]
        args = parser.parse_args(cmd)
        args = str(args).split("(")[-1]
        args = args.rstrip(")")
        args = args.split(", ")
        args = "\n".join(args)
        self.check_string(args)

    @staticmethod
    def _prepare_command_args(by_date_dir: str) -> List[str]:
        cmd = []
        cmd.append("--start_date 2021-11-23")
        cmd.append("--end_date 2021-11-25")
        cmd.append(f"--dst_dir {by_date_dir}")
        return cmd

    def _prepare_scratch_space(self) -> Tuple[str, str]:
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        hio.create_dir(by_date_dir, False)

        file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im_v2/common/data/transform/extract_data_from_db.py",
        )
        return by_date_dir, file_path
