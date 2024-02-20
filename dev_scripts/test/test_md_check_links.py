import logging
import os

import dev_scripts.md_check_links as dsmdchli
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_check_links(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test fixing link formatting in a Markdown file.
        """
        # Create a Markdown file with a variety of links.
        txt_incorrect = self._get_txt_with_incorrect_links()
        file_name = "test.md"
        file_path = self._write_input_file(txt_incorrect, file_name)
        # Run.
        mode = "fix"
        output = dsmdchli.check_links(file_path, mode)
        # Check.
        self.check_string(output)

    def test2(self) -> None:
        """
        Test reporting incorrect links in a Markdown file.
        """
        # Create a Markdown file with a variety of links.
        txt_incorrect = self._get_txt_with_incorrect_links()
        file_name = "test.md"
        file_path = self._write_input_file(txt_incorrect, file_name)
        # Run.
        mode = "check"
        output = dsmdchli.check_links(file_path, mode)
        # Make the output module-agnostic.
        amp_path = hgit.get_amp_abs_path()
        output = [m.lstrip(amp_path) for m in output]
        # Check.
        self.check_string("\n".join(output))

    def test3(self) -> None:
        """
        Test dealing with internal links in a Markdown file.
        """
        # Create a Markdown file with internal links, including TOC.
        txt_internal_links = """
<!--ts-->
   * [Best practices for writing plotting functions](#best-practices-for-writing-plotting-functions)
      * [Cosmetic requirements](#cosmetic-requirements)

<!--te-->

<!-- toc -->

- [Best practices for writing plotting functions](#best-practices-for-writing-plotting-functions)
  * [Cosmetic requirements](#cosmetic-requirements)

<!-- tocstop -->


[Data Availability](#data-availability)
        """
        file_name = "test.md"
        file_path = self._write_input_file(txt_internal_links, file_name)
        # Run the 'fix' mode.
        mode = "fix"
        output = dsmdchli.check_links(file_path, mode)
        # Check that nothing has changed.
        self.assert_equal(txt_internal_links, output)
        # Run the 'check' mode.
        mode = "check"
        output = dsmdchli.check_links(file_path, mode)
        # Check that there are no warnings.
        self.assert_equal("", "\n".join(output))

    def _get_txt_with_incorrect_links(self) -> str:
        txt_incorrect = r"""
A high-level description of KaizenFlow is
[KaizenFlow White Paper](/papers/DataFlow_stream_computing_framework/DataFlow_stream_computing_framework.pdf)

- General intro to `DataPull`
  - [/docs/datapull/ck.datapull.explanation.md](/docs/datapull/ck.datapull.explanation.md)

- Inspect RawData
  - [./im_v2/common/notebooks/Master_raw_data_gallery.ipynb](./im_v2/common/notebooks/Master_raw_data_gallery.ipynb)
  - ./im_v2/common/notebooks/Master_raw_data_gallery.ipynb
  - `.im_v2/common/data/client/im_raw_data_client.py`

- Convert data types
  - `im_v2/common/data/transform/convert_csv_to_pq.py`

- How to QA data
  - im_v2/ccxt/data/qa/notebooks/data_qa_bid_ask.ipynb

- Save PnL and trades
  - [/dataflow/model/notebooks/Master_save_pnl_and_trades.ipynb]()
  - ck.export_alpha_data.explanation.md

- A list of all the generic notebooks:
  - [docs/dataflow/ck.master_notebooks.reference.md](docs/dataflow/ck.master_notebooks.reference.md)

- Example: [datapull/ck.create_airflow_dag.tutorial.md](https://github.com/cryptokaizen/cmamp/blob/master/docs/datapull/ck.create_airflow_dag.tutorial.md)

- notebook:
  [Master_PnL_real_time_observer](https://github.com/cryptokaizen/cmamp/blob/master/oms/notebooks/Master_PnL_real_time_observer.ipynb)

To access the UI, visit [AirFlow UI](http://172.30.2.44:8090/home).

    - ../../../../amp/helpers:/app/helpers
      deleted: .github/workflows/build_image.yml.DISABLED

<img src="figs/diataxis/diataxis_summary.png">

- Here is a command for the run, log file path:
  <img src="figs/monitor_system/image1.png" style="" />

![](docs/datapull/figs/datapull/data_format.png)

![](../../defi/papers/sorrentum_figs/image14.png){width="6.854779090113736in"
height="1.2303444881889765in"}
        """
        return txt_incorrect

    def _write_input_file(self, txt: str, file_name: str) -> str:
        """
        Write test content to the file.

        :param txt: the content of the file
        :param file_name: the name of the file
        :return: the path to the file with the test content
        """
        # Get the path to the scratch space.
        dir_name = self.get_scratch_space()
        # Compile the path to the test content file.
        file_path = os.path.join(dir_name, file_name)
        file_path = os.path.abspath(file_path)
        # Create the file.
        hio.to_file(file_path, txt)
        return file_path
