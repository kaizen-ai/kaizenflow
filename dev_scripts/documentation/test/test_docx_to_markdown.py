import os
import re
import tempfile
import logging
from typing import List
import helpers.hunit_test as hunitest
from unittest.mock import patch, call
import shutil
import pytest
import dev_scripts.documentation.convert_docx_to_markdown as cdtmd

_LOG = logging.getLogger(__name__)

# #############################################################################
class Test_move_media(hunitest.TestCase):
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self):
        # Run setup
        self.set_up_test()
        yield
        # Run teardown
        self.tear_down_test()

    def set_up_test(self) -> None:
        """
        Create temporary Markdown file and figures folder for testing.
        """
        self.md_file = "test.md"
        self.md_file_figs = "test_md_file_figs"

        # Create the Markdown file.
        with open(self.md_file, 'w') as f:
            f.write("# Sample Markdown file\n")
       
        # Create the figures folder with a nested 'media' folder.
        media_folder_path = os.path.join(self.md_file_figs, "media")
        os.makedirs(media_folder_path, exist_ok=True)
       
        # Add sample files to the 'media' folder.
        img1_path = os.path.join(media_folder_path, "image1.png")
        img2_path = os.path.join(media_folder_path, "image2.jpg")

        with open(img1_path, 'w') as f:
            f.write("Sample image content")
      
        with open(img2_path, 'w') as f:
            f.write("Sample image content")
        

       

    def tear_down_test(self) -> None:
        """
        Clean up temporary files and directories created for testing.
        """
        if os.path.exists(self.md_file):
            os.remove(self.md_file)
        if os.path.exists(self.md_file_figs):
            shutil.rmtree(self.md_file_figs)
        pass

    @patch("shutil.move")
    @patch("os.listdir")
    @patch("os.path.isdir")
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))

    def test_move_media(self, mock_join, mock_isdir, mock_listdir, mock_move):
        # Prepare the mocks.
        mock_isdir.return_value = True
        mock_listdir.return_value = ["image1.png", "image2.jpg"]
        media_dir = os.path.join(self.md_file_figs, "media")
        md_file_figs = self.md_file_figs
        
        # Call the function.
        cdtmd._move_media(self.md_file_figs)
        
        # Check if the move operations were called correctly.
        expected_calls = [
            call(os.path.join(media_dir, "image1.png"), md_file_figs),
            call(os.path.join(media_dir, "image2.jpg"), md_file_figs)
        ]
        mock_move.assert_has_calls(expected_calls, any_order=True)

# #############################################################################