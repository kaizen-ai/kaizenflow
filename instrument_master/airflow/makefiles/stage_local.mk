# TODO(gp): Move to im/airflow/devops/makefile

im.docker_pull_related_images.local:
	WORKER_IMAGE=$(IM_IMAGE_AIRFLOW_DEV) \
	docker-compose -f devops/compose/docker-compose.local.yml pull


im.run_bash.local:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.local.yml \
		run \
		--rm \
        app bash


im.run_convert_s3_to_sql_kibot.local:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.local.yml \
		run \
		--rm \
        app instrument_master/app/transform/convert_s3_to_sql.py $(PARAMS)


im.docker_run_stack.local:
	WORKER_IMAGE=$(IM_IMAGE_AIRFLOW_DEV) \
	docker stack deploy \
		-c devops/compose/docker-compose.local.yml \
		--resolve-image never \
		im_airflow_stack_local


im.docker_down_stack.local:
	docker stack remove im_airflow_stack_local

# make im.run_convert_s3_to_sql_kibot.local PARAMS="--provider kibot --symbol AAPL --frequency T --contract_type continuous --asset_class stocks --exchange NYSE"