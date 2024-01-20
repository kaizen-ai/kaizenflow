<!--toc-->
   * [Extracting marketing data](#extracting-marketing-data)
      * [Tra path](#tra-path)
         * [Data Source](#data-source)
         * [Work Flow](#work-flow)
      * [Signal path](#signal-path)
         * [Data Source](#data-source-1)
         * [Work Flow](#work-flow-1)
      * [LIN path](#lin-path)


<!--tocstop-->

# Extracting marketing data

- The idea is to get detailed "marketing" information (e.g., about investors,
  employees, customers, etc) from some public/easy to collect databases
- The goal is to extract  as much detailed data as possible from those sources

- The services we use are:
  - LinkedIn/SalesNavigator
    - Search for people according to certain criteria
    - Search for groups

- We currently have two ways of generate investor details using investors work
  info:
- [Dropcontact](./dropcontact.how_to_guide.md):
- import marketing.dropcontact as mrkdrop
- `mrkdrop.get_email_from_dropcontact(first_names, last_names, company_names, API_KEY)`
- [PB](https://phantombuster.com/): Using their pipeline

## Tra path

### Data Source

- Tra search query pages that contains different VCs' name and their detail
  pages's link
- Should be downloaded as a single MIME HTML file (`.mhtml`)

### Work Flow

- See [tra guide](./tra.how_to_guide.md) for detailed usages
- `import marketing.tra as mrktra`

1. Tra search query pages -> VC names

- `mrktra.get_VCs_from_mhtml`
- VC names -> investors (PB)

2. Tra search query pages -> VC detail pages

- manually open and download the pages

3. VC detail page -> Work info (`Full Name, Company Name, Job Title`) of
   investors

- `mrktra.get_employees_from_mhtml`
- Work info -> investor details (Dropcontact)

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
