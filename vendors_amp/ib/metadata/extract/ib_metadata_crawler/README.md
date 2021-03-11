# 

- To run:

  ```bash
  # Build container.
  > cd amp/vendors_amp/ib/metadata/extract/ib_metadata_crawler
  > make ib_metadata_crawler.docker_build

  # Run crawler.
  > make ib_metadata_crawler.run
  ```

- The results are:
  ```
  scrapy.log
  src/exchanges.csv
  src/symbols.csv
  ```
