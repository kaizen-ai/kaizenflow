<!--ts-->
   * [Build image](#build-image)
   * [Run kibot app](#run-kibot-app)
      * [Run locally for development](#run-locally-for-development)
      * [Run locally only PostgreSQL](#run-locally-only-postgresql)
   * [Development flow using stages](#development-flow-using-stages)



<!--te-->

# Kibot timing

- [gdoc](https://docs.google.com/document/d/1BdOj3DGpFzHQZ6dpYCMMAeyjTtqYgltyqDbQ7n8Vde8/edit#)

- Kibot documentation (from http://www.kibot.com/Support.aspx#data_format) states
  the following timing semantic:
   "a time stamp of 10:00 AM is for a period between 10:00:00 AM and 10:00:59 AM"
   "All records with a time stamp between 9:30:00 AM and 3:59:59 PM represent
   the regular US stock market trading session."

- Thus the open price at time "ts" corresponds to the instantaneous price at time
  "ts", which by our conventions corresponds to the "end" of an interval in the
  form [a, b) interval

- As a consequence our usual "ret_0" # (price entering instantaneously at time t
  - 1 and exiting at time t) is implemented in terms of Kibot data as:
  ret_0(t) = open_price(t) - open_price(t - 1)

  ```
               datetime     open     high      low    close   vol      time  ret_0
  0 2009-09-27 18:00:00  1042.25  1043.25  1042.25  1043.00  1354  18:00:00    NaN
  1 2009-09-27 18:01:00  1043.25  1043.50  1042.75  1042.75   778  18:01:00   1.00
  ```

- E.g., ret_0(18:01) is the return realized entering (instantaneously) at 18:00
  and exiting at 18:01

- In reality we need time to:
  - compute the forecast
  - enter the position
- We can't use open at time t - 1 since this would imply instantaneous forecast
- We can use data at time t - 2, which corresponds to [t-1, t-2), although
  still we would need to enter instantaneously
- A better assumption is to let 1 minute to enter in position, so:
  - use data for [t - 2, t - 1) (which Kibot tags with t - 2)
  - enter in position between [t - 1, t)
  - capture the return realized between [t, t + 1]
- In other terms we need 1 extra delay (probably 2 would be even safer)

# Build image

1. Build release candidate image

```bash
> make docker_build_kibot_rc_image
```

2. (Optional for now) Push release candidate image to ECR (Optional for now)

```bash
> make docker_push_kibot_rc_image
```

3. Tag release candidate image with the latest tag

```bash
> make docker_tag_kibot_rc_latest
```

4. Push latest image do ECR

```bash
> make docker_push_kibot_latest_image
```

5. (Optional for now) You can tag the latest image with a various version and
   push it to ECR

```bash
> make docker_tag_kibot_latest_version VERSION=0.1
> make docker_push_kibot_version_image VERSION=0.1
```

# Run kibot app

Pull image.

```bash
> make docker_pull_kibot
```

By the default we use $KIBOT_IMAGE for all run. You can check the setup to
identify concrete image.

```bash
> make kibot_setup_print
# You will get something like:
KIBOT_REPO_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com/kibot
KIBOT_IMAGE=083233266530.dkr.ecr.us-east-2.amazonaws.com/kibot:latest
KIBOT_IMAGE_RC=083233266530.dkr.ecr.us-east-2.amazonaws.com/kibot:rc
KIBOT_IMAGE_RC_SHA=083233266530.dkr.ecr.us-east-2.amazonaws.com/kibot:bab347bceedb8cb6b013acecd0439e9cf87ba9f4
```

## Run locally for development

Basic run:

```bash
> make docker_kibot_run_local
```

## Run locally only PostgreSQL

Start server in detached mode:

```bash
> make docker_kibot_postgres_up_local
```

Stop PostgreSQL server:

```bash
> make docker_kibot_down_postgres_local
```

Stop PostgreSQL server and remove all data:

```bash
> make docker_kibot_rm_postgres_local
```

# Development flow using stages

- Use `local` stages for development locally. Related: target in makefile
  `docker_kibot_run_local`

All stages can have separate docker-compose files. All stages must have separate
targets in make file to start and stop services.
