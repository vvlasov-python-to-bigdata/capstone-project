"""
This module contains helper function to initialize SparkSession object.
"""
import os

from pyspark.sql import SparkSession

import __main__
from src.shared.logging import Log4jWrapper


def start_spark_session(app_name: str) -> tuple:
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

    :param app_name: name of Spark application.
    :return: a tuple with Spark session and Spark logger wrapper objects.
    """
    spark_builder: SparkSession.Builder = SparkSession.builder.appName(app_name)

    # Detect execution environment type
    if not (not(hasattr(__main__, '__file__')) or 'DEBUG' in os.environ.keys()):
        # Determined as local, add more parameters
        spark_builder.master("local[*]")

    # Create session and logger objects
    spark_session: SparkSession = spark_builder.getOrCreate()
    logger: Log4jWrapper = Log4jWrapper(spark_session.sparkContext)

    return (spark_session, logger)
