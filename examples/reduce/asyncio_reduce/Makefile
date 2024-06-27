TAG ?= stable
PUSH ?= false
REPO = "quay.io/numaio/numaflow-python/asyncio-reduce:${TAG}"

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	docker buildx build -t ${REPO} --platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	docker build -t ${REPO} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${REPO}; fi
