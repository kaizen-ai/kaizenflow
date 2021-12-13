import os

import pytest

import helpers.git as hgit
import helpers.io_ as hio
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im.ccxt.db.utils as imccdbuti


# TODO(Nikola): Expose `TestImDbHelper` from im_v2/common/db/utils.py instead of `setUp()` and `tearDown()` methods.
class TestExtractDataFromDb1(hunitest.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(), "im_v2/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsysinte.system(cmd, suppress_output=False)
        host = "localhost"
        dbname = "im_postgres_db_local"
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        # TODO(Nikola): Remove eventually.
        os.environ["POSTGRES_HOST"] = host
        os.environ["POSTGRES_DB"] = dbname
        os.environ["POSTGRES_PORT"] = str(port)
        os.environ["POSTGRES_USER"] = user
        os.environ["POSTGRES_PASSWORD"] = password
        hsql.wait_db_connection(host, dbname, port, user, password)
        self.connection = hsql.get_connection(
            host,
            dbname,
            port,
            user,
            password,
            autocommit=True,
        )
        # TODO(Nikola): linter is complaining about cursor and create database?
        hsql.create_database(self.connection, "test_db", overwrite=True)
        ccxt_ohlcv_table_query = imccdbuti.get_ccxt_ohlcv_create_table_query()
        ccxt_ohlcv_insert_query = """
        INSERT INTO public.ccxt_ohlcv
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
        """
        Bring down the test container.
        """
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        self.connection.close()
        hsysinte.system(cmd, suppress_output=False)
        # TODO(Nikola): Remove eventually.
        os.environ.pop("POSTGRES_HOST")
        os.environ.pop("POSTGRES_DB")
        os.environ.pop("POSTGRES_PORT")
        os.environ.pop("POSTGRES_USER")
        os.environ.pop("POSTGRES_PASSWORD")
        super().tearDown()

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
