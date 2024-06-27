TAG ?= stable
PUSH ?= false
REPO = "quay.io/numaio/numaflow-python/even-odd:${TAG}"

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	cd ../../../ && docker buildx build \
	-f examples/map/even_odd/Dockerfile \
	-t {REPO} \
	--platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	cd ../../../ && docker build \
	-f examples/map/even_odd/Dockerfile \
	-t ${REPO} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${REPO}; fi
