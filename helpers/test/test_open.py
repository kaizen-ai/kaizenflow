import logging

import helpers.open as opn
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_open1(ut.TestCase):
    """Test unknown extension and unknown systems."""

    def test1(self) -> None:
        """Test unknown extension raises an error."""
        with self.assertRaises(AssertionError) as cm:
            opn.open_file("a.unknown_ext")
        # Check error text.
        self.assertIn("unknown_ext", str(cm.exception))

    def test2(self) -> None:
        """Test unknown OS raises an error."""
        with self.assertRaises(AssertionError) as cm:
            opn._cmd_open_html("b.html", "UnknownOS")
        # Check error text.
        self.assertIn("UnknownOS", str(cm.exception))


class Test_open2(ut.TestCase):
    """Test different command correctness for opening html file."""

    def test1(self) -> None:
        """Test Linux."""
        cmd = opn._cmd_open_html("a.html", "Linux")
        self.check_string(cmd)

    def test2(self) -> None:
        """Test Windows."""
        cmd = opn._cmd_open_html("b.html", "Windows")
        self.check_string(cmd)

    def test3(self) -> None:
        """Test Darwin."""
        cmd = opn._cmd_open_html("c.html", "Darwin")
        self.check_string(cmd)


class Test_open3(ut.TestCase):
    """Test different command correctness for opening pdf file."""

    def test1(self) -> None:
        """Test Darwin."""
        cmd = opn._cmd_open_html("a.pdf", "Darwin")
        self.check_string(cmd)
