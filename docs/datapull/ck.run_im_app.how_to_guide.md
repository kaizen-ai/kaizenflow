<!--ts-->
   * [Workflows](#workflows)
      * [Run IM app](#run-im-app)
      * [Prerequisites](#prerequisites)
      * [Run locally for development](#run-locally-for-development)
      * [Run dev stage](#run-dev-stage)
      * [Stop remaining PostgreSQL containers](#stop-remaining-postgresql-containers)
   * [Development flow using stages](#development-flow-using-stages)
   * [Extracting data to S3](#extracting-data-to-s3)
      * [IB](#ib)
      * [Kibot](#kibot)
   * [Loading data into the DB](#loading-data-into-the-db)
   * [Airflow](#airflow)



<!--te-->

# Workflows

- Build the image

  ```bash
  > invoke docker_build_local_image
  ```

- Build and test the image but do not push it to the repo

<!---

## Run unit tests

- Run the tests in the base Docker image:

  ```bash
  > make docker_bash
  > pytest im
  ```

- Run the tests that require IM Docker image:
  ```bash
  > make im.run_fast_tests
  ```

## Build image

- Build release candidate image:

  ```bash
  > make im.docker_build_image.rc
  ```

- (Optional for now) Push release candidate image to ECR:

  ```bash
  > make im.docker_push_image.rc
  ```

- Tag release candidate image with the latest tag:

  ```bash
  > make im.docker_tag_rc_image.latest
  ```

- Push latest image do ECR:
  ```bash
  > make im.docker_push_image.latest
  ```

-->

## Run IM app

- Pull image.

  ```bash
  > make im.docker_pull
  ```

- By default we use $IM_IMAGE_DEV for all the runs. You can check the setup to
  identify the actual image used
  ```bash
  > make im.print_setup
  # You will get something like:
  IM_REPO_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com/im
  IM_IMAGE_DEV=083233266530.dkr.ecr.us-east-2.amazonaws.com/im:latest
  IM_IMAGE_RC=083233266530.dkr.ecr.us-east-2.amazonaws.com/im:rc
  ```

## Prerequisites

- IB TWS or Gateway app [should be up](./ib/connect/README.md) on `research.p1`
  with API port 4012

- To start IB Gateway app:

  ```bash
  > cd im/ib/connect
  > make ib_connect.docker_build_image.rc && \
    make ib_connect.docker_tag_latest.rc && \
    make ib_connect.docker_tag_prod.latest
  > IB_CONNECT_USER=gpsagg314 \
    IB_CONNECT_PASSWORD=<password> \
    IB_CONNECT_VNC_PASSWORD=12345 \
    IB_CONNECT_API_PORT=4012 \
    IB_CONNECT_VNC_PORT=5912 \
    make ib_connect.docker_up.prod
  ```

- Test that the connection to IB is up
  ```bash
  > make im.docker_up.local
  root@...:/app# ./im/devops/docker_scripts/sanity_check_ib.py
  Saving log to file '/app/im/devops/docker_scripts/sanity_check_ib.py.log'
  Shutting up 2 modules
  03-31_19:38 INFO : _main          : Connecting to 172.31.16.23:4012, attempt 1/100
  03-31_19:38 INFO : get_es_data    : Getting data for contract=Future(symbol='ES', lastTradeDateOrContractMonth='202103', exchange='GLOBEX', includeExpired=True)
  03-31_19:38 INFO : get_es_data    :                  date     open     high      low    close  volume  average  barCount
  0 2021-02-01 14:30:00  3740.75  3747.25  3717.25  3727.75  188100  3735.35     49249
  1 2021-02-01 15:00:00  3727.50  3743.00  3720.00  3731.25  243588  3732.30     65466
  2 2021-02-01 16:00:00  3731.50  3761.50  3725.50  3754.50  189778  3747.65     44627
  3 2021-02-01 17:00:00  3754.75  3768.25  3754.75  3768.00  105194  3762.50     26175
  4 2021-02-01 18:00:00  3768.00  3770.50  3761.50  3767.75   82137  3766.20     23193
  03-31_19:38 INFO : _main          : Disconnecting
  03-31_19:38 INFO : _main          : Done
  ```

## Run locally for development

- Build local image:

  ```bash
  > make im.docker_build_image.rc
  > make im.docker_tag_rc_image.latest
  ```

- Basic run with PostgreSQL:

  ```bash
  > make im.docker_up.local
  ```

- Basic run without PostgreSQL:

  ```bash
  > make im.docker_bash.local
  ```

- Execute command inside the container and close:

  ```bash
  > make im.docker_cmd.local <your command>
  ```

## Run dev stage

- Mostly the same as local stage but PostgreSQL is running on
  `pg-im.multistage.p1::5432`.

- Build dev image:
  - Same image as for local stage.

- Basic run with PostgreSQL:

  ```bash
  > make im.docker_bash.dev
  ```

- Basic run without PostgreSQL:

  ```bash
  > make im.docker_bash_without_psql.dev
  ```

- Execute command inside the container and close:

  ```bash
  > make im.docker_cmd.dev <your command>
  ```

## Stop remaining PostgreSQL containers

- Stop a container for local stage:

  ```bash
  > make im.docker_down.local
  ```

- Stop a container and remove all data for local stage:

  ```bash
  > make im.docker_rm.local
  ```

- Stop a container for dev stage:

  ```bash
  > make im.docker_down.dev
  ```

# Development flow using stages

- Use `local` stages for development locally. Related: target in makefile
  `im.docker_up.local`

- All stages can have separate docker-compose files.

  `# TODO(\*): Is it true? Reality is different.`

  All stages must have separate targets in makefile to start and stop services.

- Use `make im.docker_command.<stage> <command>` to run described above commands
  in production way.

# Extracting data to S3

- Bring up the stack:

  ```bash
  > make im.docker_up.local
  ```

## IB

- To extract data from TWS (E.g. ES minutely data since 1 Jan 2020):

  ```bash
  cd im/ib/data/extract
  download_ib_data.py \
      --symbol ES \
      --frequency T \
      --exchange GLOBEX \
      --asset_class Futures \
      --contract_type continuous \
      --currency USD  \
      --start_ts 20200101000000 \
      --incremental
  ```

- To download metadata:

  ```bash
  cd im/ib/metadata/extract
  # Build image.
  make ib_metadata_crawler.docker_build
  # Run crawler.
  make ib_metadata_crawler.run
  ```

- To extract a bunch of symbols (needed downloaded metadata):

  ```bash
  cd im/ib/data/extract
  download_ib_data.py \
      --frequency T \
      --exchange GLOBEX \
      --asset_class Futures \
      --contract_type continuous \
      --currency USD  \
      --start_ts 20200101000000 \
      --incremental
  ```

## Kibot

- Download metadata firstly:

  ```bash
  # TODO(*): Doesn't work inside the container, needed AWS CLI
  # or code refactoring to save on S3 directly. Logic part is OK.
  python im/kibot/metadata/extract/download_ticker_lists.py
  python im/kibot/metadata/extract/download_adjustments.py -u <user> -p <password>
  ```

- Extract the data from Kibot (E.g. SP500 daily data. To extract all remove
  `--dataset` argument):
  ```bash
  # TODO(*): Doesn't work inside the container, needed AWS CLI
  # or code refactoring to save on S3 directly. Logic part is OK.
  python im/kibot/data/extract/download.py -u <user> -p <password> --dataset sp_500_daily
  ```

# Loading data into the DB

- Bring up the stack:

  ```bash
  > make im.docker_up.local
  ```

- Run a test inside a container to populate some data

  ```bash
  root@6e507de35b1b:/app# cd im
  root@6e507de35b1b:/app/im# pytest -k TestSqlWriterBackend1
  ```

- To connect to the local DB:

  ```bash
  PGPASSWORD=eidvlbaresntlcdbresntdjlrs psql \
    -h localhost \
    -p 5550 \
    -d im_postgres_db_local \
    -U menjgbcvejlpcbejlc

  im_postgres_db_local=# \dt;
  List of relations
  Schema |        Name         | Type  |       Owner
  --------+---------------------+-------+--------------------
  public | exchange            | table | menjgbcvejlpcbejlc
  public | ibdailydata         | table | menjgbcvejlpcbejlc
  public | ibminutedata        | table | menjgbcvejlpcbejlc
  public | ibtickbidaskdata    | table | menjgbcvejlpcbejlc
  public | ibtickdata          | table | menjgbcvejlpcbejlc
  public | kibotdailydata      | table | menjgbcvejlpcbejlc
  public | kibotminutedata     | table | menjgbcvejlpcbejlc
  public | kibottickbidaskdata | table | menjgbcvejlpcbejlc
  public | kibottickdata       | table | menjgbcvejlpcbejlc
  public | symbol              | table | menjgbcvejlpcbejlc
  public | tradesymbol         | table | menjgbcvejlpcbejlc
  ```

- To copy data from S3 to SQL (e.g. HG continuous minutely data):

  ```bash
  cd im/app/transform
  convert_s3_to_sql.py \
      --provider ib \
      --symbol HG \
      --frequency T \
      --contract_type continuous \
      --asset_class Futures \
      --exchange NYMEX \
      --currency USD
  ```

  Or for Kibot:

  ```bash
  cd im/app/transform
  convert_s3_to_sql.py \
      --provider kibot \
      --symbol AAPL \
      --frequency T \
      --asset_class sp_500 \
      --exchange NYSE \
      --currency USD
  ```

- A bunch of symbols e.g. for NYMEX exchange:
  ```bash
  cd im/app/transform
  convert_s3_to_sql.py \
      --provider ib \
      --frequency T \
      --contract_type continuous \
      --asset_class Futures \
      --exchange NYMEX \
      --currency USD
  ```
  Or for Kibot (SP500 daily data):
  ```bash
  # TODO(*): Implement SymbolUniverse for Kibot to run app/transform/convert_s3_to_sql.py.
  cd im/kibot/data/transform
  convert_s3_to_sql_kibot.py \
      --dataset sp_500_daily \
      --exchange NYSE
  ```
  `# TODO(\*): Incremental mode is not working for Kibot.`

# Airflow

- Airflow is deployed for local stage.
  `# TODO(\*): Write DAG-s check it is working.`
