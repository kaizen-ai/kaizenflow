# Code Layout

- The code layout matches the different software components that we have

- We separate the 3 ETL stages (extract, transform, and load) for both data and
  metadata

- ```app/```
  - User interface to IM
  - It contains all and only the code that is used to access the production data
    (SQL, S3, and the conversion)
    
- ```common/```: code common to all providers
  - ```data/```: code to handle the common data
    - ```extract/```
    - ```load/```
    - ```transform/```
  - ```db/```: code to handle the DB
  
- ```devops/```: scripts to handle infrastructure, with the usual conventions
  
- ```ib/```: Interactive Broker provider
  - ```connect/```: IB TWS interface
  - ```data/```: handle IB data
    - ```extract/```
      - ```gateway/```: 
    - ```load/```
    - ```transform/```
  - ```metadata/```: Handle IB medata
    - ```extract```: IB crawler
    - ```load/```
    - ```transform/```
      
- ```kibot/```: Kibot provider
  - ```data/```
    - ```extract/```: extract the data from the website and save it to S3
    - ```load/```: load the data from S3 into SQL
    - ```transform/```: transform the data
  - ```metadata/```
    - ```extract/```
    - ```load/```
    - ```transform/```
