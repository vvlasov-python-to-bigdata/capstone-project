"""
This module contains tests for `src/jobs/task1/purchases_attribution_udf.py`.
"""
import unittest

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.jobs.task1 import purchases_attribution_udf
from src.shared.logging import Log4jWrapper
from src.shared.spark import start_spark_session


class PurchasesAttributionUDFTests(unittest.TestCase):
    """
    Tests for Spark job described in
    `src/jobs/task1/purchases_attribution_udf.py`.
    """

    def setUp(self):
        spark_session, spark_logger, config = start_spark_session(
            app_name="purchases_attribution_udf_test",
            files=["config/test/purchases_attribution_config.yaml"]
        )

        self.spark_session: SparkSession = spark_session
        self.spark_logger: Log4jWrapper = spark_logger
        self.config: dict = config

    def tearDown(self):
        if self.spark_session:
            self.spark_session.stop()

    def test__get_clickstream_attribute_value(self):
        """
        Test: Function `purchases_attribution_udf#_get_clickstream_attribute_value` works correctly.
        """
        # Input data
        dummy_clickstream_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/mobile_app_clickstream.csv",
            header=True
        )

        # Expected results
        expected_campaign_id = None
        expected_purchase_id = "2"

        # Actual results
        # pylint: disable=protected-access
        actual_empty_attributes = (
            dummy_clickstream_df
            .filter(F.col("attributes").isNull())
            .withColumn(
                "campaignId",
                purchases_attribution_udf._get_clickstream_attribute_value(
                    F.col("attributes"),
                    F.lit("campaign_id")
                )
            )
            .select(F.col("campaignId"))
        ).collect()[0][0]

        actual_data = (
            dummy_clickstream_df
            .filter(F.col("eventId") == "3")
            .withColumn(
                "campaignId",
                purchases_attribution_udf._get_clickstream_attribute_value(
                    F.col("attributes"),
                    F.lit("campaign_id")
                )
            )
            .withColumn(
                "purchaseId",
                purchases_attribution_udf._get_clickstream_attribute_value(
                    F.col("attributes"),
                    F.lit("purchase_id")
                )
            )
            .select(F.col("campaignId"), F.col("purchaseId"))
        ).collect()[0]

        # Asserts
        self.assertIsNone(actual_empty_attributes)
        self.assertEqual(expected_campaign_id, actual_data[0])
        self.assertEqual(expected_purchase_id, actual_data[1])

    def test__unique_id(self):
        """
        Test: Function `purchases_attribution_udf#_unique_id` works correctly.
        """
        # Input data
        dummy_clickstream_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/mobile_app_clickstream.csv",
            header=True
        )

        # pylint: disable=protected-access
        actual_result = (
            dummy_clickstream_df
            .withColumn(
                "sessionId",
                purchases_attribution_udf._unique_id()
            )
            .select("sessionId")
        ).collect()[0][0]

        self.assertIsNotNone(actual_result)

if __name__ == "__main__":
    unittest.main()
