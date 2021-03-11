BUILD_DIR=_dist
ARTIFACT_NAME=jobs.zip

all: build

build: clean
	mkdir -p $(BUILD_DIR)
	find ./src/jobs -type f -exec cp {} ./$(BUILD_DIR) \;
	cp -r ./src/shared ./$(BUILD_DIR)/shared
	zip -r ./$(BUILD_DIR)/$(ARTIFACT_NAME) ./$(BUILD_DIR)/**
	find ./$(BUILD_DIR)/* ! -name '$(ARTIFACT_NAME)' -exec rm -rf {} +

test:

clean: clean-build clean-test

clean-build:
	rm -rf $(BUILD_DIR)

clean-test:

help:
	@echo "Usage:"
	@echo "   * clean - remove all build and tests artifacts"
	@echo "   * build - archive all jobs and dependent packages to one archive. \
	Put result to ${BUILD_DIR}/${ARTIFACT_NAME}"
	@echo "   * test  - run tests"