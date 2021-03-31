# #############################################################################
# MULTISTAGE.
# #############################################################################
im-app.docker_up.local:
	docker-compose \
		-f amp/apps/im/compose/docker-stack.multistage.yml
		run \
		--rm \
		-l user=$(USER) \
		app \
		bash

# Run app containers.
im-app.docker_bash:
	docker-compose \
		-f amp/apps/im/compose/docker-stack.multistage.yml
		run \
		--rm \
		-l user=$(USER) \
		--no-deps \
		app \
		bash

# Stop multistage container including all dependencies.
im-app.docker_down.local:
	docker-compose \
		-f amp/apps/im/compose/docker-stack.multistage.yml
		down \
		--remove-orphans