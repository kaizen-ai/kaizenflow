<!--ts-->
   * [Run unit tests](#run-unit-tests)
   * [Build image](#build-image)
   * [Run IM app](#run-im-app)
      * [Prerequisites](#prerequisites)
      * [Run locally for development](#run-locally-for-development)
      * [Stop remaining PostgreSQL containers](#stop-remaining-postgresql-containers)
   * [Development flow using stages](#development-flow-using-stages)
   * [Loading data into the DB](#loading-data-into-the-db)



<!--te-->

# Run unit tests

- Run the tests in the base Docker image:

  ```bash
  > pytest vendors_amp
  ```

- Run the tests that require IM Docker image:
  ```bash
  > make im.run_fast_tests
  ```

# Build image

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

# Run IM app

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
  with API port 4012. For example:

  ```bash
  > IB_CONNECT_USER=gpsagg314 \
    IB_CONNECT_PASSWORD=<password> \
    IB_CONNECT_VNC_PASSWORD=12345 \
    IB_CONNECT_API_PORT=4012 \
    IB_CONNECT_VNC_PORT=5912 \
    make ib_connect.docker_up.prod
  ```

- Testing that the connection to IB is up
  ```bash
  > make im.docker_up.local
  root@...:/app# ./instrument_master/devops/docker_scripts/sanity_check_ib.py
  Saving log to file '/app/instrument_master/devops/docker_scripts/sanity_check_ib.py.log'
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
  > make im.docker_bash
  ```

## Stop remaining PostgreSQL containers

- Stop a container:

  ```bash
  > make im.docker_down.local
  ```

- Stop a container and remove all data:
  ```bash
  > make im.docker_rm.local
  ```

# Development flow using stages

- Use `local` stages for development locally. Related: target in makefile
  `im.docker_up.local`

- All stages can have separate docker-compose files. All stages must have
  separate targets in makefile to start and stop services.

# Loading data into the DB

- Bring up the stack:

  ```bash
  > make im.docker_up.local
  ```

- Run a test inside a container to populate some data

  ```bash
  root@6e507de35b1b:/app# cd instrument_master
  root@6e507de35b1b:/app/instrument_master# pytest -k TestSqlWriterBackend1
  ```

- To connect to the local DB:

  ```bash
  PGPASSWORD=eidvlbaresntlcdbresntdjlrs psql -h localhost -p 5550 -d im_postgres_db_local  -U menjgbcvejlpcbejlc

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
  cd instrument_master/app/transform
  python convert_s3_to_sql.py \
                --provider ib \
                --symbol HG \
                --frequency T \
                --contract_type continuous \
                --asset_class Futures \
                --exchange NYMEX
  ```
