<!--ts-->
   * [Build image](#build-image)
   * [Run kibot app](#run-kibot-app)
      * [Run locally for development](#run-locally-for-development)
      * [Run locally only PostgreSQL](#run-locally-only-postgresql)
   * [Development flow using stages](#development-flow-using-stages)



<!--te-->

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
