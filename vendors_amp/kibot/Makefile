# #############################################################################
# Postgres only.
# #############################################################################

# Start local postgres server.
kibot_local_up_postgres:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		up \
		-d \
		kibot_postgres_local

# Stop local postgres server.
kibot_local_down_postgres:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		down -v

# #############################################################################
# Kibot vendor.
# #############################################################################

# Run container with local database.
# Use for development.
kibot_local_bash:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app bash


