<!--toc-->
   * [Extracting marketing data](#extracting-marketing-data)
      * [Tracxn path](#tra-path)
         * [Data Source](#data-source)
         * [Work Flow](#work-flow)
      * [Signal path](#signal-path)
         * [Data Source](#data-source-1)
         * [Work Flow](#work-flow-1)
      * [LIN path](#lin-path)


<!--tocstop-->

# Extracting marketing data

- The idea is to extract detailed "marketing" information (e.g., about
  investors, employees, customers, etc) from some public/easy to collect
  databases

- The services we use are:
  - LinkedIn/SalesNavigator
    - Search for people according to certain criteria (e.g., job description,
      working at a certain company)
    - Search for groups
  - Tracxn
    - Get information about company
  - Dropcontact
    - Find emails for people
  - Signal NFX
    - Find information for investors
  - PhantomBuster
    - Extract information from LinkedIn/SalesNavigator
    - Send messages on LinkedIn
    - Autoconnect to people on LinkedIn
  - Yamm
    - Plugin to automate sending and tracking emails
  - Docsend
    - Share and track documentation

# How to extract data from Tracxn

## Intro

- Tracxn website is at https://tracxn.com
- E.g., searching for VC firms that invested in Series seed and A of AI 
companies
  https://tracxn.com/a/s/query/t/investors/t/activeinestorsvc/table?h=f7aa3582c404433be03c12f1df4f0e463c78bc6398c81a45a403ac8703e0ba0a&s=sort%3DinvestmentCount%7Corder%3DDESC
- The result is a set of links to company pages like
  - https://tracxn.com/a/companies/srAiTt8Aevx0dkPbmrFdUVl21azd7Gx7AOT8J4fO1Zs/ycombinator.com/people/currentteam
  - This page contains information about people working at that company with
    information about LinkedIn and emails

### Workflow

- `import marketing.tra as mrktra`

1. Tracxn search query pages

    - `mrktra.get_VCs_from_mhtml`
    - VC names -> investors (PB)

2. Tracxn search query pages -> VC detail pages

- manually open and download the pages

3. VC detail page -> Work info (`Full Name, Company Name, Job Title`) of
   investors

- `mrktra.get_employees_from_mhtml`
- Work info -> investor details (Dropcontact)

## Locations

- The module is located at `marketing/tra`
- Example notebooks are at:
  - `marketing/notebooks/SorrTask601_Extract_people_from_Tra_company_html.ipynb`
  - `marketing/notebooks/SorrTask601_Extract_VCs_from_Tra_search_mhtml.ipynb`

## Dataflow

### Input

- An MIME HTML file containing:
  1. A table of VC company names
  2. A list of investors from the company

### Output

- According to the input type, output can be:
  1. A pandas dataframe containing the VC names and its information
  2. A pandas dataframe containing the investors' `First Name`, `Last Name` and
     their linkedin profiles link

## Download Data

- We want to use only static data as the source of our extraction code. To
  download the data page:
  1. In any modern browser, open a page containing required data
  2. Use the browser's `Save As` button to download the webpage as a
     `Web page, single file`. Or use any other download method that can fulfill
     the requirement in step 3
  3. If you see the downloaded file format is `.mht` or `.mhtml`, you can
     process forward. Otherwise you won't be able to bypass the check layer from
     the website
  4. Save the `.mhtml` file and its path will work as input data

## Usage

- Import the module using `import marketing.tra as mrktra`
- Each file in this module only have one public function. It takes the
  downloaded MIME HTML (`.mhtml`) file path as parameter and returns a pandas
  dataframe containing the extracted data.
- In any notebook, run the following script to get a pandas dataframe
  representing the data:

  ```python
  import marketing.tra as mrktra

  # For VC extraction:
  VCs_mhtml_path = "<VCs_mthml_file_path>"
  VCs_dataframe = mrktra.get_VCs_from_mhtml(VC_mhtml_path)
  # For employee list extraction:
  employees_html_path = "<employees_html_path>"
  employees_dataframe = mrktra.get_employees_from_mhtml(employees_html_path)

  ```

## Signal path

### Data Source

- Signal NFX contains many lists, each of them has the
- Select a Signal investors list URL from
  `https://signal.nfx.com/investor-lists/`

### Work Flow

- See [signal guide](./signal.how_to_guide.md) for detailed usages
- `import marketing.signal as mrksign`

1. Signal list URL -> Work info (`Full Name, Company Name, Job Title`) of
   investors in that field

- `mrksign.extract_investors_from_signal_url`
- Work info of investors -> investor details (Dropcontact)

## LIN path

- This path is not feasible for automation, as they ban accounts very fast.
  Even if we are just doing queries manually, but too frequently.
