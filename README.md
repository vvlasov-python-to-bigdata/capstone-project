[![Coverage Status](https://coveralls.io/repos/github/vvlasov-python-to-bigdata/capstone-project/badge.svg?branch=main)](https://coveralls.io/github/vvlasov-python-to-bigdata/capstone-project?branch=main)

# Python to BigData: Capstone Project

* [Overview](#overview)
* [Solution Description](#solution-description)
* [Usage](#usage)
  * [Prerequisites](#prerequisites)
  * [Set up for development](#set-up-for-development)
  * [Running the tests](#running-the-tests)
  * [Build](#build)
  * [Run on local Spark](#run-on-local-spark)
  * [Run on remote Spark cluster](#run-on-remote-spark-cluster)
* [Documentation](#documentation)

## Overview

This is a final project of _Python Engineer to BigData Engineer_ LP 
including marketing analytics BigData tasks on sample dataset. The solution is made using `PySpark v3.1.1`.

#### List of tasks:
* **Task #1.1.** [Build purchases attribution projection (using default SparkSQL)](#build-purchases-attribution-projection-using-default-sparksql) [**[Source code]**](/src/jobs/task1/purchases_attribution_sql.py)
* **Task #1.2.** [Build purchases attribution projection (using custom UDF)](#build-purchases-attribution-projection-using-custom-udf)
* **Task #2.1.** [Calculate top marketing campaigns by revenue](#calculate-top-marketing-campaigns-by-revenue) [**[Source code]**](/src/jobs/task2/top_marketing_campaigns_sql.py)
* **Task #2.2.** [Calculate the most popular marketing channel](#calculate-the-most-popular-marketing-channel) [**[Source code]**](/src/jobs/task2/most_popular_channels_sql.py)

#### Additional tasks:
* **Task #2.1(2).** Calculate top marketing campaigns by revenue (using Spark DataFrame API only) [**[Source code]**](/src/jobs/task2/top_marketing_campaigns_df_api.py)
* **Task #2.2(2).** Calculate the most popular marketing channel (using Spark DataFrame API only) [**[Source code]**](/src/jobs/task2/most_popular_channels_df_api.py)

## Solution Description

This section describes solution details for each task.

### Build purchases attribution projection (using default SparkSQL)

### Build purchases attribution projection (using custom UDF)

### Calculate top marketing campaigns by revenue

### Calculate the most popular marketing channel

## Usage

This section describes how to build, run and work with the project.

There is a `Makefile` to setup development environment, build and run tests. You can run 
```
make help
```
to see all available commands with the `Makefile`.

### Prerequisites

> **NOTE**
>
> Is is recommended to use `MacOS` or `Linux` to build or run this porject.
>
> But if you are using `Windows` make sure you have `make` command
> installed from `GNUWin32` package: http://gnuwin32.sourceforge.net/packages/make.htm.

Please make sure you have installed the following:

* `Python 3.7+`
* `PySpark v3.1.1+`
* [`pipenv`](https://pypi.org/project/pipenv/)
  <details>
    <summary>How to install</summary>
  
    <code>python -m pip install pipenv</code>
  </details>

### Setup

Execute the following command to set up the project:
```
make venv
```

This command makes the following:
* Checks that `pipenv` installed
* Creates Python virtual environment in `.venv` folder
* Install all project dependencies to the virtual environment

### Running the tests

Run the following command to execute all tests:
```
make test
```

### Build

There is an ability to pack all jobs files and its dependencies into one ZIP archive called `jobs.zip` to use it on remote Spark cluster.

To do this run the command:
```
make build
```

This command makes the following:
* Clean up `_dist` folder
* Download all Python dependencies to `_dist` folder
* Copy all files from `src/jobs` and `src/shared` to `_dist` folder
* Create archive called `jobs.zip`

### Run on local Spark

To run a certain Spark job on local cluster, use the following commands:
```
make venv
pipenv run python <PATH_TO_JOB_FILE>
```
where 
* `<PATH_TO_JOB_FILE>` - relative path to Python file describing a Spark job, e.g. 'src/jobs/task1/purchases_attribution_sql.py'

By default in local run mode all jobs uses test configuration specific for the job (you can browse [./config/test](./config/test) to find it). Test configuration uses test data which is 5 random files from each input dataset.

To use other configuration (e.g. [production configuration](./config/prod) which uses whole datasets), add the path to the configuration file while running a job:
```
make venv
pipenv run python <PATH_TO_JOB_FILE> <PATH_TO_CONFIG_FILE>
```

For example:
```
pipenv run python src/jobs/task1/purchases_attribution_sql.py config/prod/purchases_attribution_config.yaml
```

### Run on remote Spark cluster

After [building](#build) the archive with jobs and its dependencies you can run a job with `spark-submit` command:
```
$SPARK_HOME/bin/spark-submit \
    --master <YOUR_MASTER_NODE> \
    --py-files packages.zip \
    --files <PATH_TO_CONFIG_FILE> \
    <PATH_TO_JOB_FILE>
```
where:
* `<YOUR_MASTER_NODE>` - a Spark cluster master node address. See [Spark Master URLs](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
* `<PATH_TO_CONFIG_FILE>` - relative path to the job's configuration file in YAML format. The file should be ended with `_config.yaml`, e.g. 'config/prod/purchases_attribution_config.yaml'
* `<PATH_TO_JOB_FILE>` - relative path to Python file describing a Spark job, e.g. 'src/jobs/task1/purchases_attribution_sql.py'

For example, command to run `purchases_attribution_sql.py` job:
```
make build
$SPARK_HOME/bin/spark-submit \
    --master yarn> \
    --py-files packages.zip \
    --files config/prod/purchases_attribution_config.yaml \
    src/jobs/task1/purchases_attribution_sql.py
```

> **NOTE**
>
> When running a job on remote Spark cluster, you **should** use 
> a configuration file with at least paths to input data and 
> output directory.

## Documentation

This section contains list of additional documentation.