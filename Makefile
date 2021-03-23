BUILD_DIR=_dist
ARTIFACT_NAME=jobs.zip

# Builds one Zip archive with source code and dependencies
build: clean make-build-dir deps
	find ./src/jobs ! -name __init__.py -type f -exec cp {} ./$(BUILD_DIR) \;
	cp -r ./src/shared ./$(BUILD_DIR)/shared
	cd ./$(BUILD_DIR) && zip -r ./$(ARTIFACT_NAME) .
	find ./$(BUILD_DIR)/* ! -name '$(ARTIFACT_NAME)' -exec rm -rf {} +

# Downloads dependencies to $(BUILD_DIR) folder
deps: is-pipenv-installed is-piplock-exists venv
	pipenv lock -r > requirements.txt
	touch requirements.txt
	pipenv run pip install -r requirements.txt --target ./_dist
	rm -rf requirements.txt

# Makes Pipenv environment and install all dev dependencies
venv: is-pipenv-installed is-piplock-exists
	PIPENV_VENV_IN_PROJECT=enabled pipenv install --dev

# Runs tests
test:
	@. .venv/bin/activate && nosetests --config=.noserc -v tests

# Linting source code and tests
lint: is-venv-exists
	@. .venv/bin/activate && pylint --output-format colorized src/shared/** src/jobs/**

# Removes all trash files
clean: clean-build clean-test

clean-build:
	rm -rf $(BUILD_DIR)

clean-test:

make-build-dir:
	mkdir -p $(BUILD_DIR)

# Checks that pipenv tool is installed, throws error otherwise
is-pipenv-installed:
ifeq (,$(shell which pipenv))
	$(error "ERROR - pipenv is not installed. Suggesting run `pip3 install pipenv` to load pipenv into global site packages or install via a system package manager")
endif

# Checks that Pipefile.lock is exist, throws error otherwise
is-piplock-exists:
ifeq (,$(wildcard Pipfile.lock))
	$(error "ERROR - cannot find Pipfile.lock")
endif

# Checks that Python virtual environment is exist inside .venv folder
is-venv-exists:
ifeq (,$(wildcard .venv/bin/python))
	$(error "ERROR - cannot find virtual environment in .venv folder. Suggesting run `make venv` to create and setup Python virtual environment for the project")
endif

help:
	@echo "Usage:"
	@echo "   * clean - remove all build and tests artifacts"
	@echo "   * build - archive all jobs and dependent packages to one archive. \
	Put result to ${BUILD_DIR}/${ARTIFACT_NAME}"
	@echo "   * venv  - create Python virtual environment and install all dependencies \
	(including [dev-packages] in Pipfile])."
	@echo "             The environment is created in .venv folder inside the project."
	@echo "   * lint  - check source code and tests with pylint"
	@echo "   * test  - run tests"