
DOCKER_IMAGE ?= 'eventsflow-dev:0.1.2-ubuntu'

.PHONY: console
console:
	@ docker run -ti --rm --name eventsflow-console \
		-v $(shell pwd):/app ${DOCKER_IMAGE} \
		-c bash

.PHONY: test
test: 
	@ drone exec --event test

.PHONY: release
release:
	@ drone exec --event release

