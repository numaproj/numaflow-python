TAG ?= stable
PUSH ?= false

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	cd ../../../ && docker buildx build \
	-f examples/sideinput/simple_sideinput/Dockerfile \
	-t "quay.io/numaio/numaflow-python/sideinput-example:${TAG}" \
	--platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	cd ../../../ && docker build \
	-f examples/sideinput/simple_sideinput/Dockerfile \
	-t "quay.io/numaio/numaflow-python/sideinput-example:${TAG}" .
	@if [ "$(PUSH)" = "true" ]; then docker push "quay.io/numaio/numaflow-python/sideinput-example:${TAG}"; fi
