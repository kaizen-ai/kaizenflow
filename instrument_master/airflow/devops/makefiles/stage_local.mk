# TODO(*): Use a different repo like im-airflow or call the images airflow-latest ?
IM_IMAGE_AIRFLOW_DEV=$(IM_REPO_BASE_PATH):latest-airflow

# Use Docker buildkit or not.
# DOCKER_BUILDKIT=1
DOCKER_BUILDKIT=0

im.docker_build_worker_image:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) \
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IM_IMAGE_AIRFLOW_DEV) \
		--file devops/docker_build/im_db_loader_worker.dev.Dockerfile \
		.

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
