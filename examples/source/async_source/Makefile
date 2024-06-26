TAG ?= stable
PUSH ?= false

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	cd ../../../ && docker buildx build \
	-f examples/source/async_source/Dockerfile \
	-t "quay.io/numaio/numaflow-python/async-source:${TAG}" \
	--platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	cd ../../../ && docker build \
	-f examples/source/async_source/Dockerfile \
	-t "quay.io/numaio/numaflow-python/async-source:${TAG}" .
	@if [ "$(PUSH)" = "true" ]; then docker push "quay.io/numaio/numaflow-python/async-source:${TAG}"; fi
