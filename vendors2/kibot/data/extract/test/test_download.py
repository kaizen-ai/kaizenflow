import os

import helpers.unit_test as hut
import vendors2.kibot.data.extract.download as kb_dd


class TestKibotDownload(hut.TestCase):
    def test_extract_dataset_links(self) -> None:
        """
        Test that extraction of dataset links from my account page works.
        """
        file_name = "my_account.html"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        actual = kb_dd.DatasetListExtractor.extract_dataset_links(file_name)
        self.check_string(actual.to_csv())

    def test_extract_payload_links(self) -> None:
        """
        Test that extraction of payload links from a dataset page works.
        Use all_stocks_1min as an example
        """
        file_name = "all_stocks_1min.html"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        actual = kb_dd.DatasetExtractor._extract_payload_links(file_name)
        self.check_string(actual.to_csv(), use_gzip=True)
