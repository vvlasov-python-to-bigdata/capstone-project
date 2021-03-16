"""
This module contains tests for `shared.logging` module.
"""
import unittest

from src.shared.spark import start_spark_session


class Log4jWrapperTests(unittest.TestCase):
    """
    Test suite for shared.logging.Log4jWrapper class tests.
    """
    def setUp(self):
        spark_session, logger, _ = start_spark_session("test-app")
        self.logger = logger
        self.spark_session = spark_session

    def tearDown(self):
        self.spark_session.stop()

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
