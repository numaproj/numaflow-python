TAG ?= stable
PUSH ?= false

.PHONY: update
update:
	poetry update -vv

.PHONY: image-push
image-push: update
	cd ../../../ && docker buildx build \
	-f examples/sourcetransform/event_time_filter/Dockerfile \
	-t "quay.io/numaio/numaflow-python/mapt-event-time-filter:${TAG}" \
	--platform linux/amd64,linux/arm64 . --push

.PHONY: image
image: update
	cd ../../../ && docker build \
	-f examples/sourcetransform/event_time_filter/Dockerfile \
	-t "quay.io/numaio/numaflow-python/mapt-event-time-filter:${TAG}" .
	@if [ "$(PUSH)" = "true" ]; then docker push "quay.io/numaio/numaflow-python/mapt-event-time-filter:${TAG}"; fi
