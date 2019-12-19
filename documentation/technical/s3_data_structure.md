<!--ts-->
   * [Description of folder structure on S3.](#description-of-folder-structure-on-s3)



<!--te-->
# Description of folder structure on S3.

1. We thinking about data in terms of vendors.
   - `vendor`
     - Data source. E.g. twitter, eia, kibot, etc.
   - `vendor/`
     - Root for all data connected to a particular vendor
     - E.g.,
     ```
          twitter/
      twitter/
          eia/
          kibot/
     ```
     - The same structure we have in the codebase.

2. Each vendor directory can contain next folders.
   - `datasets`
   - `vendor/**datasets**/dataset_1`
     - Particular dataset and all versions of it, e.g.,

     ```bash
     twitter/datasets/accounts/
                     accounts_v1.pkl
                     accounts_v2.pkl
                     accounts_v3.pkl
                     accounts_v4.pkl
     ```
   - `config_files`
   - `vendor/$config_files`
     - Files that we use for nlp scripts, or else additional data that we needed
     - E.g.,
       ```bash
       twitter/config_files/oil_sentiment/
                       v1/breakers.csv
                       v2/important_features.csv
                       v3/breakers.csv
                       v3/important_features.csv
       ```
   - There should be an understanding that dataset not equal to config_files,
     but some datasets can be used as config_files(no needs to copy them in
     config_files folder in that case).
   - `production`
   - `vendor/production/service_name_version`
     - Data that protected from writing. If we want to freeze some data for a
       particular project, we should put it in this folder with the same
       structure to be able to track where from it was taken. We can run tests
       using these paths and alert changes if needed.
     - E.g.,
       ```bash
       twitter/production/oil_sentiment_historical_pipeline_v1.0/
           /datasets/accounts/
                           accounts_v4.pkl
           /config_files/oil_sentiment/
                           v1/breakers.csv
                           v1/important_features.csv
           /config_files/irelevance_model/
                          Task134_CatBoost_irrelevance_classifier_20190709.pkl
       ```
