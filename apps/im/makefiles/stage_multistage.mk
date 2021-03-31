# #############################################################################
# MULTISTAGE.
# #############################################################################
im-app.docker_up.multistage:
	docker-compose \
		-f apps/im/compose/docker-stack.multistage.yml \
		up -d

# Stop multistage container including all dependencies.
im-app.docker_down.multistage:
	docker-compose \
		-f apps/im/compose/docker-stack.multistage.yml \
		down