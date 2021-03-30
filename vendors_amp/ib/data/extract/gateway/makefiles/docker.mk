# Pull image from ECR.
ib_extract.docker_pull:
		$(foreach v, $(ID_EXTRACT_RELATED_IMAGES), docker pull $(v); )


## #############################################################################
## Build.
## #############################################################################

# Build images.
ib_extract.docker_build.rc_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IB_EXTRACT_RC_IMAGE) \
		-t $(IB_EXTRACT_RC_IMAGE_SHA) \
		--file vendors_amp/ib/data/extract/gateway/dev.Dockerfile .

# Push release candidate images.
ib_extract.docker_push.rc_image:
	docker push $(IB_EXTRACT_RC_IMAGE)
	docker push $(IB_EXTRACT_RC_IMAGE_SHA)

# Tag :rc image with :dev tag.
ib_extract.docker_tag.rc_dev:
	docker tag $(IB_EXTRACT_RC_IMAGE) $(IB_EXTRACT_DEV_IMAGE)

# Push image with :dev tag.
ib_extract.docker_push.dev_image:
	docker push $(IB_EXTRACT_DEV_IMAGE)

# Build images.
ib_extract.docker_build.prod_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IB_EXTRACT_PROD_IMAGE) \
		--file vendors_amp/ib/data/extract/gateway/prod.Dockerfile .

# Push prod images.
ib_extract.docker_push.prod_image:
	docker push $(IB_EXTRACT_PROD_IMAGE)

# Get docker logs.
ib_extract.docker.logs:
	docker logs compose_tws_1
