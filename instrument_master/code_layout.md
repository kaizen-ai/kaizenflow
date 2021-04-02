# Code Layout

- The code layout matches the different software components that we have

- We separate the 3 ETL stages (extract, transform, and load) for both data and
  metadata

- ```app/```
  - User interface to IM
  - It contains all and only the code that is used to access the production data
    (SQL, S3, and the conversion)
    
- ```common/```
  - Code common to all providers
  - ```data/```
    - Code to handle the common data
    - ```extract/```
    - ```load/```
    - ```transform/```
  - ```db/```
    - Code to handle the DB
  
- ```devops/```
  - Scripts to handle infrastructure, with the usual conventions
  
- ```ib/```
  - Interactive Broker provider
  - ```connect/```
    - IB TWS interface
  - ```data/```
    - Handle IB data
  - ```metadata/```
    - Handle IB medata
    - ```extract```
      - Crawler
      
- ```kibot/```
  - Kibot provider
  - ```data/```
    - ```extract/```
      - Extract the data from the website and save it to S3
    - ```load/```
      - Load the data from S3 to 
    - ```transform/```
  - ```metadata/```
    - ```extract/```
    - ```load/```
    - ```transform/```