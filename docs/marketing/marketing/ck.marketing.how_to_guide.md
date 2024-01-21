

<!-- toc -->

- [Extracting marketing data](#extracting-marketing-data)
- [Tracxn](#tracxn)
  * [Introduction](#introduction)
  * [Code](#code)
  * [Workflow](#workflow)
    + [Company flow](#company-flow)
    + [People flow](#people-flow)
- [Dropcontact](#dropcontact)
  * [Introduction](#introduction-1)
  * [Code](#code-1)
  * [Dataflow](#dataflow)
  * [Signal NFX](#signal-nfx)
  * [Introduction](#introduction-2)
  * [Code](#code-2)
    + [Work Flow](#work-flow)

<!-- tocstop -->

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

<!-- ####################################################################### -->

# Tracxn

## Introduction

- Tracxn website is at https://tracxn.com
- E.g., searching for VC firms that invested in Series seed and A of AI
  companies
  https://tracxn.com/a/s/query/t/investors/t/activeinestorsvc/table?h=f7aa3582c404433be03c12f1df4f0e463c78bc6398c81a45a403ac8703e0ba0a&s=sort%3DinvestmentCount%7Corder%3DDESC
- The result is a set of links to company pages like
  - Https://tracxn.com/a/companies/srAiTt8Aevx0dkPbmrFdUVl21azd7Gx7AOT8J4fO1Zs/ycombinator.com/people/currentteam
  - This page contains information about people working at that company with
    information about LinkedIn and emails

## Code

- The module is located at `marketing/tracxn`
- Example notebooks are at:
  - `marketing/tracxn/notebooks/SorrTask601_Extract_VCs_from_Tra_search_mhtml.ipynb`
  - `marketing/tracxn/notebooks/SorrTask601_Extract_people_from_Tra_company_html.ipynb`

## Workflow

### Company flow

1. Go to a Tracxn VCs search result page
2. Use the browser's `Save As` button to download the webpage as a
   `Web page, single file`.
3. If you see the downloaded file format is `.mht` or `.mhtml`, you can process
   forward. Otherwise, you won't be able to bypass the check layer from the
   website.
   - The MIME HTML file containing a table of VC company names
4. Call `get_VCs_from_mhtml()` method with the `.mhtml` file path to extract the
   result as a Pandas dataframe containing the VC names and its information:
   ```txt
   Investor Name                                              Jiangmen Ventures
   Score                                                                      2
   #Rounds                                                                   10
   Portfolio Companies                    DMAI;Bito Robotics;DEEP INFORMATICS++
   Investor Location                                                   Chaoyang
   Stages of Entry                                    Series A (8);Seed (6)[+2]
   Sectors of Investment        Enterprise Applications (10);High Tech (9)[+16]
   Locations of Investment                     China (27);United States (4)[+3]
   Company URL                https://tracxn.com/a/companies/3pfRjux26cdu4Aq...
   ```
5. Save the returned dataframe to whatever format preferred

An example of this flow is
`marketing/tracxn/notebooks/SorrTask601_Extract_VCs_from_Tra_search_mhtml.ipynb`

The inputs are in
https://drive.google.com/drive/u/2/folders/1nT5CYuFWLOxb10ONw7yjfzyobc9VM90- The
outputs are in
https://drive.google.com/drive/u/2/folders/1MfTeNHR7sxex_rSpyp54HeJsxtLjKqy3

### People flow

- Same flow as above but it converts a page like
  https://tracxn.com/a/d/investor/srAiTt8Aevx0dkPbmrFdUVl21azd7Gx7AOT8J4fO1Zs/ycombinator.com/people/currentteam
  into a dataframe like:
  ```txt
  Name                                                 Matanya Horowitz
  LinkedIn Profile    https://linkedin.com/in/matanya-horowitz-87805519
  ```

<!-- ####################################################################### -->

# Dropcontact

## Introduction

- Dropcontact https://www.dropcontact.com/ is a service to find people's emails
  from their first and last name

## Code

- The code is located at `marketing/dropcontact`
- An example notebook is at
  `marketing/drpcontact/notebooks/SorrTask606_Get_email_from_dropcontact.ipynb`
- It contains a full process for extracting data using `DropContact` API and
  adding into an existing Google sheet

## Workflows

- The input is
  - Three sequences representing `First Name`, `Last Name`, `Company Name`
  - API key for dropcontact

- The output is
  - DataFrame containing all data available from DropContact API
  - Empty string is set for data not present

- Import the module using `import marketing.dropcontact as mrkdrop`
- The module provides a single function for extraction of data for `DropContact`
  API
- It takes three Sequence of data (can be python list, pandas column, etc.) and
  an API key for dropcontact
- The processing time is ~50 investors per minute
- In any notebook, run the following script to get a pandas dataframe
  representing the data returned from DropContact:

  ```python
  import marketing.dropcontact as mrkdrop

  API_KEY = <DropContact_API_KEY>
  first_names = <First Name Sequence>
  last_names = <Last Name Sequence>
  company_names = <Company Name Sequence>
  dropcontact_dataframe = mrkdrop.get_email_from_dropcontact(first_names, last_names, company_names, API_KEY)
  ```

<!-- ####################################################################### -->

## Signal NFX

## Introduction

- Signal NFX contains many lists of investors for different stage (e.g.,
  pre-seed, seed, Series A) and verticals (e.g., AI), e.g.,
  `https://signal.nfx.com/investor-lists/`

## Code

- The module is located at `marketing/signal_nfx`
- An example notebook is at
  `marketing/signal_nfx/notebooks/SorrTask612_Get_information_from_Signal.ipynb`

### Workflows

- Import the module using `import marketing.signal as mrksign`
- Select a list in https://signal.nfx.com/investor-lists/ and open its page
- The url for the list will work as a input data, for e.g.:
  `baseurl = https://signal.nfx.com/investor-lists/top-fintech-seed-investors`
- Determine the range of data to be extracted in a particular run by specifying
  the start index and the length of the data
  - This is because the page is only loading a few items for one click on the
    loading button and the total length of data is unknown. We don't want the
    code to run forever
- A Pandas dataframe with investors' `First Name`, `Last Name` and
  `Company Name`
