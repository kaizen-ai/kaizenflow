# #############################################################################
# MULTISTAGE.
# #############################################################################
im-app.docker_up.multistage:
	docker-compose \
		-f apps/im/compose/docker-compose.multistage.yml \
		up -d

# Stop multistage container including all dependencies.
im-app.docker_down.multistage:
	docker-compose \
		-f apps/im/compose/docker-compose.multistage.yml \
		down