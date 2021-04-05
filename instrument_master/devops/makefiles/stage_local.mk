im.run_bash:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.local.yml \
		run \
		--rm \
        app bash


im.run_convert_s3_to_sql_kibot:
	IMAGE=$(IM_IMAGE_DEV) \
	docker-compose \
		-f devops/compose/docker-compose.local.yml \
		run \
		--rm \
        app instrument_master/app/transform/convert_s3_to_sql.py $(PARAMS)



# make im.run_convert_s3_to_sql_kibot PARAMS="--provider kibot --symbol AAPL --frequency T --contract_type continuous --asset_class stocks --exchange NYSE"