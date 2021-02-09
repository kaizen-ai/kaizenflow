kibot_local_up_postgres:
	docker-compose \
    -f compose/docker-compose.yml \
    -f compose/docker-compose.local.yml \
		up \
		-d \
		kibot_postgres_local

kibot_local_down_postgres:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		down

kibot_local_bash:
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.local.yml \
		run --rm \
		-l user=$(USER) \
		kibot_app bash


