"""
This module contains tests for `shared.spark` module.
"""
import unittest

from pyspark.sql import SparkSession

from src.shared.spark import start_spark_session
from src.shared.logging import Log4jWrapper


class StartSparkSessionTests(unittest.TestCase):
    """
    Test suite for shared.spark.start_spark_session() function tests.
    """
    def test_start_spark_session(self):
        """
        Test: Function start_spark_session() creates Spark session.
        """
        res = start_spark_session("test-app")

        self.assertIsNotNone(res, "")
        self.assertEqual(2, len(res), "")

        spark_session: SparkSession = res[0]
        self.assertTrue(isinstance(spark_session, SparkSession), "")
        self.assertEqual("local[*]", spark_session.sparkContext.master)

        logger: Log4jWrapper = res[1]
        self.assertTrue(isinstance(logger, Log4jWrapper), "")

if __name__ == "__main__":
    unittest.main()
