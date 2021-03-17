# InteractiveBroker Crawler

The InteractiveBroker Crawler (IB Parser) allows downloading metadata about:

- Global markets metadata (country, market name, products, operation hours) are available on the platform.

- Market products metadata (market, product, IB symbol, description, symbol, currency).

The entrypoint of data extraction - https://ndcdyn.interactivebrokers.com/en/index.php?f=1562

## Installation

The IB Crawler is avaliable at the Particle Amazon ECR as a Docker image

- Make sure that you pull the latest verson of image:

```bash
docker pull 083233266530.dkr.ecr.us-east-2.amazonaws.com/ib-crawler:1.0.0
```

Or you can build an image in-place

```bash
docker build -t ib-crawler:local .
```

## Configuration

Map host folder into container `/outcome` for getting crawler results.

```bash
docker run ... -v $(pwd)/outcome:/outcome ...
```

The result of crawling is present in two `csv` files (with `tab` as a delimiter):

- `exchanges-timestamp.csv`

```csv
region	country	market	link	products	hours
North America	United States	Absolute Funds	http://absoluteadvisers.com	N/A	9:30 - 15:59 ET
North America	United States	Bats BYX (BYX)	http://www.batstrading.com/byx/	Stocks,Warrants	8:00 - 17:00 ET
North America	United States	ArcaEdge (ARCAEDGE)	http://www.nyse.com/markets/nyse-arca	Stocks (OTCBB),Warrants	Monday - Friday: 8:00-16:00
North America	United States	CME (GLOBEX)	http://www.cmegroup.com	Futures (Agriculture, Currency, Energy, Equity Index, Fixed Income),Futures Options (Currency, Equity Index),Indices	Sunday - Friday: 1700 - 1600
```

- `symbols-timestamp.csv`

```csv
market	product	s_title	ib_symbol	symbol	currency
ArcaEdge (ARCAEDGE)	ETFs	ASIA BROADBAND INC	AABB	AABB	USD
ArcaEdge (ARCAEDGE)	ETFs	ABERDEEN INTERNATIONAL INC.	AABVF	AABVF	USD
ArcaEdge (ARCAEDGE)	ETFs	AAC TECHNOLOGIES H-UNSPON AD	AACAY	AACAY	USD
ArcaEdge (ARCAEDGE)	ETFs	AURORA SOLAR TECHNOLOGIES IN	AACTF	AACTF	USD
ArcaEdge (ARCAEDGE)	ETFs	AMERICA GREAT HEALTH	AAGH	AAGH	USD
ArcaEdge (ARCAEDGE)	ETFs	AIA GROUP LTD-SP ADR	AAGIY	AAGIY	USD
```

## Run example

```bash
docker run --rm -v $(pwd)/outcome:/outcome 083233266530.dkr.ecr.us-east-2.amazonaws.com/ib-crawler:1.0.0 scrapy crawl ibroker
```

## Release History

1.0.0 - The first relase with basic functionality.
