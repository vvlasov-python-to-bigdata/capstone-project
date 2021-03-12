BUILD_DIR=_dist
ARTIFACT_NAME=jobs.zip

# Builds one Zip archive with source code and dependencies
build: clean make-build-dir deps
	find ./src/jobs -type f -exec cp {} ./$(BUILD_DIR) \;
	cp -r ./src/shared ./$(BUILD_DIR)/shared
	zip -r ./$(BUILD_DIR)/$(ARTIFACT_NAME) ./$(BUILD_DIR)/**
	find ./$(BUILD_DIR)/* ! -name '$(ARTIFACT_NAME)' -exec rm -rf {} +

# Downloads dependencies to $(BUILD_DIR) folder
deps: is-pipenv-installed is-piplock-exists
	pipenv lock -r > requirements.txt
	touch requirements.txt
	pip3 install -r requirements.txt --target ./_dist
	rm -rf requirements.txt

# Makes Pipenv environment and install all dev dependencies
deps-dev: is-pipenv-installed is-piplock-exists
	PIPENV_VENV_IN_PROJECT=enabled pipenv install --dev

# Runs tests
test:

# Removes all trash files
clean: clean-build clean-test

clean-build:
	rm -rf $(BUILD_DIR)

clean-test:

make-build-dir:
	mkdir -p $(BUILD_DIR)

# Cheks that pipenv tool is installed, throws error otherwise
is-pipenv-installed:
ifeq (,$(shell which pipenv))
	$(error "ERROR - pipenv is not installed. Suggesting run `pip3 install pipenv` to load pipenv into global site packages or install via a system package manager")
endif

# Cheks that Pipefile.lock is exist, throws error otherwise
is-piplock-exists:
ifeq (,$(wildcard Pipfile.lock))
	$(error "ERROR - cannot find Pipfile.lock")
endif

help:
	@echo "Usage:"
	@echo "   * clean    - remove all build and tests artifacts"
	@echo "   * build    - archive all jobs and dependent packages to one archive. \
	Put result to ${BUILD_DIR}/${ARTIFACT_NAME}"
	@echo "   * deps-dev - create Python virtual environment and install all dependencies \
	(including [dev-packages] in Pipfile])."
	@echo "                The environment is created in .venv folder inside the project."
	@echo "   * test     - run tests"