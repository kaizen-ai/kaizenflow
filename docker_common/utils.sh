build_container_image() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    # Prepare build area.
    #tar -czh . | docker build $OPTS -t $IMAGE_NAME -
    DIR="tmp.build"
    if [[ -d $DIR ]]; then
        rm -rf $DIR
    fi;
    cp -Lr . $DIR || true
    # Build container.
    export DOCKER_BUILDKIT=1
    #export DOCKER_BUILDKIT=0
    OPTS="--progress plain $@"
    (cd $DIR; docker build $OPTS -t $FULL_IMAGE_NAME . 2>&1 | tee ../docker_build.log; exit ${PIPESTATUS[0]})
    # Report build version.
    (cd $DIR; docker run --rm -it -v $(pwd):/data $FULL_IMAGE_NAME bash -c "/data/version.sh 2>&1 | tee /data/docker_build.version.log")
    #
    docker image ls $REPO_NAME/$IMAGE_NAME
    echo "*****************************"
    echo "SUCCESS"
    echo "*****************************"
}


remove_container_image() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    docker image ls | grep $FULL_IMAGE_NAME
    docker image ls | grep $FULL_IMAGE_NAME | awk '{print $1}' | xargs -n 1 -t docker image rm -f
    docker image ls
}


push_container_image() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    docker login --username $REPO_NAME --password-stdin <~/.docker/passwd.$REPO_NAME.txt
    docker images $FULL_IMAGE_NAME
    docker push $FULL_IMAGE_NAME
}


pull_container_image() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    docker pull $FULL_IMAGE_NAME
}


kill_container() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    docker container ls
    #
    CONTAINER_ID=$(docker container ls -a | grep $FULL_IMAGE_NAME | awk '{print $1}')
    echo "CONTAINER_ID=$CONTAINER_ID"
    if [[ ! -z $CONTAINER_ID ]]; then
        docker container rm -f $CONTAINER_ID
        docker container ls
    fi;
}


exec_container() {
    FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME
    echo "FULL_IMAGE_NAME=$FULL_IMAGE_NAME"
    docker container ls
    #
    CONTAINER_ID=$(docker container ls -a | grep $FULL_IMAGE_NAME | awk '{print $1}')
    echo "CONTAINER_ID=$CONTAINER_ID"
    docker exec -it $CONTAINER_ID bash
}
