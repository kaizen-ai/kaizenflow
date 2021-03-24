import datetime
import os

import pandas as pd
import pytest

import helpers.unit_test as hut
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.common.db.init as vcdini
import vendors_amp.kibot.data.load.sql_data_loader as vkdlsq
import vendors_amp.kibot.sql_writer_backend as vksqlw


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


