import os

import helpers.unit_test as hut
import im.common.db.create_schema as icdcrsch


class TestDbSchemaFile(hut.TestCase):
    def test_exist1(self) -> None:
        """
        Test that files with SQL schema exist.
        """
        for file_name in icdcrsch.get_sql_files():
            self.assertTrue(os.path.exists(file_name))
