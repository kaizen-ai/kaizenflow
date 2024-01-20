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

# Tracxn

## Intro

- Tracxn website is at https://tracxn.com
- E.g., searching for VC firms that invested in Series seed and A of AI 
companies
  https://tracxn.com/a/s/query/t/investors/t/activeinestorsvc/table?h=f7aa3582c404433be03c12f1df4f0e463c78bc6398c81a45a403ac8703e0ba0a&s=sort%3DinvestmentCount%7Corder%3DDESC
- The result is a set of links to company pages like
  - https://tracxn.com/a/companies/srAiTt8Aevx0dkPbmrFdUVl21azd7Gx7AOT8J4fO1Zs/ycombinator.com/people/currentteam
  - This page contains information about people working at that company with
    information about LinkedIn and emails

## Workflow

### Company flow

1. Go to a Tracxn VCs search result page
2. Use the browser's `Save As` button to download the webpage as a `Web page,
   single file`.
3. If you see the downloaded file format is `.mht` or `.mhtml`, you can process
   forward. Otherwise, you won't be able to bypass the check layer from the
   website.
    - The MIME HTML file containing a table of VC company names
4. Call `get_VCs_from_mhtml()` method with the `.mhtml` file path to extract
   the result as a Pandas dataframe containing the VC names and its information:
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

The inputs are in https://drive.google.com/drive/u/2/folders/1nT5CYuFWLOxb10ONw7yjfzyobc9VM90-
The outputs are in https://drive.google.com/drive/u/2/folders/1MfTeNHR7sxex_rSpyp54HeJsxtLjKqy3

### People flow

- Same flow as above but it converts a page like
    https://tracxn.com/a/d/investor/srAiTt8Aevx0dkPbmrFdUVl21azd7Gx7AOT8J4fO1Zs/ycombinator.com/people/currentteam
  into a dataframe like:
    ```txt
    Name                                                 Matanya Horowitz
    LinkedIn Profile    https://linkedin.com/in/matanya-horowitz-87805519
    ```

## Code

- The module is located at `marketing/tracxn`
- Example notebooks are at:
  - `marketing/tracxn/notebooks/SorrTask601_Extract_VCs_from_Tra_search_mhtml.ipynb`
  - `marketing/tracxn/notebooks/SorrTask601_Extract_people_from_Tra_company_html.ipynb`

## Signal NFX path

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
