import pytest
import tempfile

import dev_scripts.linter2.p1_doc_formatter as p1docf
import helpers.io_ as io_
import helpers.unit_test as hut


@pytest.mark.skip(reason="Disabled because of AmpTask508")
class Test_docformatter(hut.TestCase):
    def _docformatter(self, text: str) -> str:
        """Create a temporary python file with the 'text', apply docformatter,
        then return the modified content.

        :param text: content to be formatted
        :return: modified content after formatting
        """
        tmp = tempfile.NamedTemporaryFile(suffix=".py")
        io_.to_file(file_name=tmp.name, lines=text)
        p1docf._DocFormatter().execute(file_name=tmp.name, pedantic=0)
        content: str = io_.from_file(file_name=tmp.name)
        tmp.close()
        return content

    def test1(self) -> None:
        """Test that module docstring should be dedented."""
        text = '''
"""     Test 1.

    Test 2."""
        '''

        expected = '''
"""Test 1.

Test 2.
"""
'''

        actual = self._docformatter(text=text)
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test that a dot is added at the end."""
        text = '''
"""this is a test"""
        '''

        expected = '''
"""this is a test."""
'''

        actual = self._docformatter(text=text)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Tests that single quotes are replaced by double quotes."""
        text = """
'''this is a test.'''
"""

        expected = '''
"""this is a test."""
'''

        actual = self._docformatter(text=text)
        self.assertEqual(expected, actual)

    def test4(self) -> None:
        """Tests that the first line starts after the quotes if it fits in a
        single line, and no other changes are made."""
        text = '''
def sample_method() -> None:
    """
    Process NaN values in a series according to the parameters.

    :param srs: pd.Series to process
    :param mode: method of processing NaNs
        - None - no transformation
        - "ignore" - drop all NaNs
        - "ffill" - forward fill not leading NaNs
        - "ffill_and_drop_leading" - do ffill and drop leading NaNs
        - "fill_with_zero" - fill NaNs with 0
        - "strict" - raise ValueError that NaNs are detected
    :param info: information storage
    :return: transformed copy of input series
    """
'''

        expected = '''
def sample_method() -> None:
    """Process NaN values in a series according to the parameters.

    :param srs: pd.Series to process
    :param mode: method of processing NaNs
        - None - no transformation
        - "ignore" - drop all NaNs
        - "ffill" - forward fill not leading NaNs
        - "ffill_and_drop_leading" - do ffill and drop leading NaNs
        - "fill_with_zero" - fill NaNs with 0
        - "strict" - raise ValueError that NaNs are detected
    :param info: information storage
    :return: transformed copy of input series
    """
'''

        actual = self._docformatter(text=text)
        self.assertEqual(expected, actual)
