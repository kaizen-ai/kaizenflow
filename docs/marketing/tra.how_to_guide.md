# How to extract data from Tra

<!-- toc -->

- [Example Notebook(s)](#example-notebooks)
- [Dataflow](#dataflow)
- [Data downloading](#data-downloading)
- [Usage](#usage)

<!-- tocstop -->

## Example Notebook(s)

- Example notebooks are at:
  - `marketing/notebooks/SorrTask601_Extract_people_from_Tra_company_html.ipynb`
  - `marketing/notebooks/SorrTask601_Extract_VCs_from_Tra_search_mhtml.ipynb`

## Dataflow

- Input: An MIME HTML file containing:
  1. A table of VC company names
  2. A list of investors from the company.
- Output:
  1. A pandas dataframe containing the VC names and its information.
  2. A pandas dataframe containing the investors' `First Name`, `Last Name` and
     their linkedin profiles link.

## Data downloading

- We want to use only static data as the source of our extraction code. Follow
  the guidelines below to get the data page downloaded.
  1. In any modern browser, open a page containing required data.
  2. Use the browser's `Save As` button to download the webpage as a
     `Web page, single file`. Or use any other download method that can fulfill
     the requirement in step 3.
  3. If you see the downloaded file format is `.mht` or `.mhtml`, you can
     process forward. Otherwise you won't be able to bypass the check layer from
     the website.
  4. Save the `.mhtml` file and put its path into the notebook.

## Usage

- Import the module using `import marketing.tra as mrktra`
- Each file in this module only have one public function. It takes the
  downloaded MIME HTML (`.mhtml`) file path as parameter and returns a pandas
  dataframe containing the extracted data.
- In any notebook, run the following script to get a pandas dataframe
  representing the data:

  ```python
  import marketing.tra as mrktra

  source_mhtml_path = "<mthml_file_path>"
  tra_dataframe = mrktra.<extraction_function>(source_mhtml_path)
  ```
