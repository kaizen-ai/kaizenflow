# Pull image from ECR.
ib_connect.docker_pull:
		$(foreach v, $(ID_CONNECT_RELATED_IMAGES), docker pull $(v); )


## #############################################################################
## Build.
## #############################################################################

# Build images.
ib_connect.docker_build.rc_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IB_CONNECT_RC_IMAGE) \
		-t $(IB_CONNECT_RC_IMAGE_SHA) \
		--file vendors_amp/ib/connect/dev.Dockerfile .

# Push release candidate images.
ib_connect.docker_push.rc_image:
	docker push $(IB_CONNECT_RC_IMAGE)
	docker push $(IB_CONNECT_RC_IMAGE_SHA)

# Tag :rc image with :dev tag.
ib_connect.docker_tag.rc_dev:
	docker tag $(IB_CONNECT_RC_IMAGE) $(IB_CONNECT_DEV_IMAGE)

# Push image with :dev tag.
ib_connect.docker_push.dev_image:
	docker push $(IB_CONNECT_DEV_IMAGE)

# Build images.
ib_connect.docker_build.prod_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IB_CONNECT_PROD_IMAGE) \
		--file vendors_amp/ib/connect/prod.Dockerfile .

# Push prod images.
ib_connect.docker_push.prod_image:
	docker push $(IB_CONNECT_PROD_IMAGE)

# Get docker logs.
ib_connect.docker.logs:
	docker logs compose_tws_1
