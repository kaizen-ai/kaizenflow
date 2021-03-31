# #############################################################################
# Development.
# #############################################################################

# Run app container, start a dev PostgreSQL DB.
im-app.docker_up.dev:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f amp/apps/im/compose/docker-stack.dev.yml
		run \
		--rm \
		-l user=$(USER) \
		app \
		bash

# Run app container w/o PostgreSQL.
im-app.docker_bash:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f amp/apps/im/compose/docker-stack.dev.yml
		run \
		--rm \
		-l user=$(USER) \
		--no-deps \
		app \
		bash

# Stop local container including all dependencies.
im-app.docker_down.dev:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f amp/apps/im/compose/docker-stack.dev.yml
		down \
		--remove-orphans

# Stop dev container including all dependencies and remove all data.
im-app.docker_rm.dev:
	IMAGE=$(IM_IMAGE_DEV) \
	POSTGRES_PORT=${IM_PG_PORT_LOCAL} \
	docker-compose \
		-f amp/apps/im/compose/docker-stack.dev.yml
		down \
		--remove-orphans; \
	docker volume rm compose_im_postgres_data_local

# Pull docker image from AWS ECR
im-app.docker_pull:
	docker pull $(IM_IMAGE_DEV)