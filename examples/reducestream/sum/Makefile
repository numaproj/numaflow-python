TAG ?= stable
PUSH ?= false
REPO = quay.io/numaio/numaflow-python/reduce-stream-sum:${TAG}
DOCKER_FILE_PATH = examples/reducestream/sum/Dockerfile

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	cd ../../../ && docker buildx build \
	-f ${DOCKER_FILE_PATH} \
	-t ${REPO} \
	--platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	cd ../../../ && docker build \
	-f ${DOCKER_FILE_PATH} \
	-t ${REPO} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${REPO}; fi
