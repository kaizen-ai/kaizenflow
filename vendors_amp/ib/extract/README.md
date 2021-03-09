<!--ts-->
   * [Interactive Brokers Data Extractor Docker](#interactive-brokers-data-extractor-docker)
      * [Getting Started](#getting-started)
         * [Pull image](#pull-image)
         * [Create image](#create-image)
         * [Start app on localhost](#start-app-on-localhost)
         * [Start app on production](#start-app-on-production)
         * [Shutdown app](#shutdown-app)



<!--te-->

# Interactive Brokers Data Extractor Docker

## Getting Started

- All commands need to be executed from `amp` root directory

### Pull image

```bash
> make docker_login
> make ib_extract.docker_pull
```

- If the image is not available in ECR you can follow the instructions in
  [Create image](create-image)

### Create image

```bash
> make ib_extract.docker_build.rc_image
> # Check that image is correct.
> make ib_extract.docker_tag.rc_dev
```

### Start app on localhost

- Local extractor app includes connector app

- You need to add a file with IB credentials to `~/.vnc/ib.credentials` like:

  ```bash
  TWSUSERID=user123
  TWSPASSWORD=password456
  FIXUSERID=
  FIXPASSWORD=
  ```

- Start extractor container

  ```bash
  > make ib_extract.docker_run.local
  ```

- You will now also have the IB Gateway app running on port 4006 and VNC
  on 5905.

### Start app on production

- Assume IB production app is
  [already started](../connect/README.md#start-app-on-production) and your IP in
  `IB_CONNECT_TRUSTED_IPS`
  [list](../connect/README.md#additional-start-parameters)

- Make sure, that ib_connect app IP is correct at `.env/prod.env`

- Start extractor app

  ```bash
  > make ib_extract.docker_run.prod
  ```

### Shutdown app

- Local image

  ```bash
  > make ib_extract.docker_down.local
  ```

- Production image
  ```bash
  > make ib_extract.docker_down.prod
  ```
