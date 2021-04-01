# Code Layout

- Based on the container diagram, the code layout matches the different containers
  that we have

```text
app/
  # user interface to use IM
common/
  # code common to all the providers
  data/
    # code to handle the common data
    extract/
    load/
    transform
devops/
  # scripts to handle infrastructure
ib/
  # Interactive Broker provider
  connect/
    # IB TWS interface
  data/
    # Handle IB data
  metadata/
    # Handle IB medata
    extract
      # Crawler
      
kibot
  # Kibot provider
  data/
    extract/
      # Extract the data from the website and save it to S3
    load/
      # Load the data from S3 to 
    transform/
      # 
  metadata/
    extract/
    load/
    transform/
```
    

types/  # standardized types (see data model above)
loader/  # adapts all vendor loaders to a common interface
vendors/
   kibot/
      extractor/
      transformer/
      loader/
      types/  # vendor specific types
      config.py  # s3 paths, credentials for vendor
   eoddata/
      extractor/
      transformer/
      loader/
      types/
      config.py
```