"""
This module contains tests for `shared.logging` module.
"""
import unittest

from pyspark.sql import SparkSession

from src.shared.spark import start_spark_session
from src.shared.logging import Log4jWrapper


class Log4jWrapperTests(unittest.TestCase):
    """
    Test suite for shared.logging.Log4jWrapper class tests.
    """
    def setUp(self):
        _, logger = start_spark_session("test-app")
        self.logger = logger

    def test_log_info(self):
        """
        Test: Can write an info message using Log4jWrapper.
        """
        self.logger.info("Test info")

    def test_log_warning(self):
        """
        Test: Can write a warning using Log4jWrapper.
        """
        self.logger.warning("Test warning")

    def test_log_error(self):
        """
        Test: Can write an error using Log4jWrapper.
        """
        self.logger.error("Test error")

if __name__ == "__main__":
    unittest.main()
