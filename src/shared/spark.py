"""
This module contains helper function to initialize SparkSession object.
"""
from typing import List

import os
import yaml

from pyspark import SparkFiles
from pyspark.sql import SparkSession

import __main__
from .logging import Log4jWrapper


CONFIG_FILE_SUFFIX = "_config.yaml"


def start_spark_session(app_name: str, files: List[str] = None) -> tuple:
    """
    Starts Spark session and initialize Spark Log4j logger wrapper.
    Returns session and logger objects as tuple.

    The function determines running environment - local (e.g is ran from IDE
    or as Python file) or Spark cluster (via spark-submit command)
    and creates Spark session depends on it. In case of using spark-submit,
    only `app_name` argument will be applied. In case of local usage,
    applies also `master` argument as 'local[*]'. To determine
    execution type, the function checks if it is being run from inside an
    interactive console session or from an environment which has a `DEBUG`
    environment variable set as '1' (e.g. by default in Visual Studio Code).

    The function looks for a files ending with '_config.yaml' that can be sent
    with the Spark job. If it is found, it is opened, the contents parsed
    (assuming it contains valid YAML) into a dict of ETL job configuration
    parameters, which are returned as the last element in the tuple
    returned by this function. If the file cannot be found then the return
    tuple only contains the Spark session and Spark logger objects
    and None for config.

    :param app_name: name of Spark application.
    :param files: list of files to send to Spark cluster (master and workers).
    :return: a tuple with Spark session and Spark logger wrapper objects and
             config dict if presented.
    """
    files = files or []
    spark_builder: SparkSession.Builder = SparkSession.builder.appName(app_name)

    # Detect execution environment type
    if not (not(hasattr(__main__, '__file__')) or 'DEBUG' in os.environ.keys()):
        # Determined as local, add more parameters
        spark_builder.master("local[*]")

        # Add additional files
        spark_builder.config('spark.files', ','.join(files))

        # Save events log for history
        spark_builder.config('spark.eventLog.enabled', True)

    # Create session and logger objects
    spark_session: SparkSession = spark_builder.getOrCreate()
    logger: Log4jWrapper = Log4jWrapper(spark_session.sparkContext)

    # Get configuration from config files
    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in os.listdir(spark_files_dir)
                    if filename.endswith(CONFIG_FILE_SUFFIX)]

    if config_files:
        config_dict: dict = {}
        for cfg_file in config_files:
            path_to_config_file = os.path.join(spark_files_dir, cfg_file)
            with open(path_to_config_file, 'r') as config_file:
                config_dict = {**config_dict, **yaml.safe_load(config_file)}
            logger.warning(f"parsed config file: {cfg_file}")
    else:
        logger.info('no config files found')
        config_dict = None

    return (spark_session, logger, config_dict)
