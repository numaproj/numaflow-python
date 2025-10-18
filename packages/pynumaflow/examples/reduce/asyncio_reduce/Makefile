TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = "quay.io/numaio/numaflow-python/asyncio-reduce:${TAG}"

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	docker buildx build -t ${IMAGE_REGISTRY} --platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	docker build -t ${IMAGE_REGISTRY} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi
