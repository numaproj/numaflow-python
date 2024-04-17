TAG ?= testv1
PUSH ?= true

.PHONY: image-push
image-push:
	docker buildx build -t "quay.io/numaio/numaflow-python/sink-log:${TAG}" --platform linux/amd64,linux/arm64 . --push

.PHONY: image
image:
	docker build --no-cache -t "quay.io/kohlisid/numaflow-python/sink-log:${TAG}" .
	@if [ "$(PUSH)" = "true" ]; then docker push "quay.io/kohlisid/numaflow-python/sink-log:${TAG}"; fi

.PHONY: clean
clean:
	rm -rf ./dist
