"""
This module contains tests for `shared.spark` module.
"""
import unittest

from pyspark.sql import SparkSession

from src.shared.spark import start_spark_session
from src.shared.logging import Log4jWrapper


class StartSparkSessionTests(unittest.TestCase):
    """
    Test suite for shared.spark.start_spark_session() function tests
    """
    def test_start_spark_session(self):
        """
        Test: Function start_spark_session() creates Spark session
        """
        res = start_spark_session("test-app")

        self.assertIsNotNone(res, "")
        self.assertEqual(3, len(res), "")

        spark_session: SparkSession = res[0]
        self.assertTrue(isinstance(spark_session, SparkSession), "")
        self.assertEqual("local[*]", spark_session.sparkContext.master)

        logger: Log4jWrapper = res[1]
        self.assertTrue(isinstance(logger, Log4jWrapper), "")

        self.assertIsNone(res[2])

        spark_session.stop()

    def test_start_spark_session_with_config(self):
        """
        Test: Function start_spark_session() loads config from file
        """
        spark_session, _, config = start_spark_session(
            "test-app", files=["config/test/dummy_config.yaml"])

        self.assertIsNotNone(config)
        self.assertTrue("input" in config)

        spark_session.stop()

if __name__ == "__main__":
    unittest.main()
