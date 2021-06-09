<!--ts-->
   * [InteractiveBrokers (IB) metadata crawler](#interactivebrokers-ib-metadata-crawler)
      * [Installation](#installation)
      * [Configuration](#configuration)
      * [Release History](#release-history)



<!--te-->

# InteractiveBrokers (IB) metadata crawler

- The InteractiveBrokers crawler allows downloading metadata about products that
  are available on the platform:
  - Global markets metadata (e.g., country, market name, products, operation
    hours)
  - Market products metadata (e.g., market, product, IB symbol, description,
    symbol, currency)

- The entrypoint of data extraction is
  https://ndcdyn.interactivebrokers.com/en/index.php?f=1562

## Installation

- You can build an image in-place with `make`
  ```bash
  > make ib_metadata_crawler.docker_build
  ```

## Configuration

- Use `make` command for running

  ```bash
  > make ib_metadata_crawler.run
  ```

- The current work directory on the host will map into container folder
  `/outcome` that contains the crawler results:

  ```bash
  > docker run \
    --rm
    -v $(pwd)/outcome:/outcome \
    ib-crawler:1.0.0 \
    scrapy crawl ibroker
  ```

- The result of crawling is present in two `csv` files, with `tab` as a
  delimiter:

- `exchanges-timestamp.csv`

  ```csv
  region          country          market               link                                     products                    hours
  North America   United States    Absolute Funds       http://absoluteadvisers.com              N/A                         9:30 - 15:59 ET
  North America   United States    Bats BYX (BYX)       http://www.batstrading.com/byx           Stocks,Warrants             8:00 - 17:00 ET
  North America   United States    ArcaEdge (ARCAEDGE)  http://www.nyse.com/markets/nyse-arca    Stocks (OTCBB),Warrants     Monday - Friday: 8:00-16:00
  North America   United States    CME (GLOBEX)         http://www.cmegroup.com                  Futures (Agriculture, Currency, Energy, Equity Index, Fixed Income),Futures Options (Currency, Equity Index),Indices    Sunday - Friday: 1700 - 1600
  ```

- `symbols-timestamp.csv`
  ```csv
  market                 product  s_title                       ib_symbol     symbol    currency
  ArcaEdge (ARCAEDGE)    ETFs     ASIA BROADBAND INC            AABB          AABB      USD
  ArcaEdge (ARCAEDGE)    ETFs     ABERDEEN INTERNATIONAL INC.   AABVF         AABVF     USD
  ArcaEdge (ARCAEDGE)    ETFs     AAC TECHNOLOGIES H-UNSPON AD  AACAY         AACAY     USD
  ArcaEdge (ARCAEDGE)    ETFs     AURORA SOLAR TECHNOLOGIES IN  AACTF         AACTF     USD
  ArcaEdge (ARCAEDGE)    ETFs     AMERICA GREAT HEALTH          AAGH          AAGH      USD
  ArcaEdge (ARCAEDGE)    ETFs     AIA GROUP LTD-SP ADR          AAGIY         AAGIY     USD
  ```

## Release History

1.0.0 - The first relaase with basic functionality
