kibot_local_run_postgres:
	docker-compose \
		-f compose/docker-compose.local.yml \
		run --rm \
		-d \
		-l user=$(USER) \
		kibot_postgres_local

kibot_local_down_postgres:
	docker-compose \
		-f compose/docker-compose.local.yml \
		down