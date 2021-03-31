import os

import helpers.unit_test as hut
import instrument_master.common.db.init as vcdini


class TestDbSchemaFile(hut.TestCase):
    """
    Test SQL initialization file existence.
    """

    def test_exist1(self) -> None:
        """
        Test that schema SQL file exists.
        """
        for file_name in vcdini.get_init_sql_files():
            self.assertTrue(os.path.exists(file_name))
