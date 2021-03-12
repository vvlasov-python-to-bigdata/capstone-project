"""
This module contains a wrapper for the Spark log4j logger,
enabling Log4j logging from Python jobs code.
"""
from pyspark import SparkConf, SparkContext


class Log4jWrapper():
    """
    Wrapper class for Log4j Spark logger.

    :param spark_context: Spark context object.
    """
    def __init__(self, spark_context: SparkContext):
        conf: SparkConf = spark_context.getConf()
        log4j = spark_context._jvm.org.apache.log4j
        message_prefix = f"<{conf.get('spark.app.id')} {conf.get('spark.app.name')}>"

        self._log4j_logger = log4j.LogManager.getLogger(message_prefix)

    def info(self, message) -> None:
        """
        Logs an information message.

        :param message: message to write to log.
        :return: None
        """
        self._log4j_logger.info(message)

    def warning(self, message) -> None:
        """
        Logs a warning.

        :param message: warning message to write to log.
        :return: None
        """
        self._log4j_logger.warn(message)

    def error(self, message) -> None:
        """
        Logs an error.

        :param message: error message to write to log.
        :return: None
        """
        self._log4j_logger.error(message)
