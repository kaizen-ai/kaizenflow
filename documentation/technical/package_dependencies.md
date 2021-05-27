# Finding dependencies

- To find the dependencies of the code

  ```bash
  > pip install pipreqs

  > pipreqs --ignore notebooks,gallery,old,...,etl3/fetchers,dbn_importer/helpers

  > more requirements.txt
  PyWavelets==1.1.1
  Scrapy==2.2.0
  Theano==1.0.4
  beautifulsoup4==4.9.3
  boto3==1.14.22
  botocore==1.17.22
  dataclasses==0.8
  ```
